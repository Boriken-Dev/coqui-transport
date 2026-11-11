//! Ending a process together with everything it started, which std cannot do: a job object on
//! Windows, a process group on Unix.

use std::io;
use std::process::{Child, Command};

use super::Exit;

/// How a poll of the child found it.
pub(super) enum Poll {
    Running,
    /// Exited and not yet collected, so its id and its group's are still its own.
    #[cfg_attr(windows, allow(dead_code))]
    Exited(Exit),
    /// Exited and collected: the platform could not look without collecting.
    #[cfg_attr(
        all(
            unix,
            not(any(
                target_os = "cygwin",
                target_os = "horizon",
                target_os = "openbsd",
                target_os = "redox",
                target_os = "wasi"
            ))
        ),
        allow(dead_code)
    )]
    Collected(Exit),
}

pub(super) fn exit_of(status: std::process::ExitStatus) -> Exit {
    #[cfg(unix)]
    let signal = std::os::unix::process::ExitStatusExt::signal(&status);
    #[cfg(not(unix))]
    let signal = None;
    Exit::new(status.code(), signal)
}

#[cfg(unix)]
pub(super) use self::unix::{Tree, contain, end, poll, prepare};
#[cfg(windows)]
pub(super) use self::windows::{Tree, contain, end, poll, prepare};

#[cfg(unix)]
mod unix {
    use super::{Child, Command, Exit, Poll, io};

    /// A process group needs no object of its own: the child leads it.
    pub(in super::super) struct Tree;

    /// The child leads a process group of its own, which `end` signals. A console is a Windows
    /// notion.
    pub(in super::super) fn prepare(command: &mut Command, _console: bool) {
        std::os::unix::process::CommandExt::process_group(command, 0);
    }

    pub(in super::super) fn contain(_child: &Child) -> io::Result<Tree> {
        Ok(Tree)
    }

    /// Kills the group, and on Linux and Android first stops and then kills every process
    /// descended from the child, those that left the group or session included. Called while the
    /// child is not yet collected: a collected child's id, and its group's, may be another
    /// process's.
    pub(in super::super) fn end(_tree: &Tree, child: &Child) {
        use rustix::process::{Pid, Signal, kill_process, kill_process_group};
        let Some(group) = i32::try_from(child.id())
            .ok()
            .filter(|pid| *pid > 1)
            .and_then(Pid::from_raw)
        else {
            return;
        };
        let escaped = descendants::stopped(group);
        let _ = kill_process_group(group, Signal::KILL);
        for pid in escaped {
            let _ = kill_process(pid, Signal::KILL);
        }
    }

    /// Looks at the child without collecting it, where the platform allows.
    #[cfg(not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi",
    )))]
    pub(in super::super) fn poll(child: &mut Child) -> io::Result<Poll> {
        use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};
        let pid = i32::try_from(child.id())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| io::Error::other("not a process id"))?;
        let status = waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        )?;
        Ok(status.map_or(Poll::Running, |status| {
            Poll::Exited(Exit::new(status.exit_status(), status.terminating_signal()))
        }))
    }

    /// Where it cannot look without collecting, polling collects.
    #[cfg(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi",
    ))]
    pub(in super::super) fn poll(child: &mut Child) -> io::Result<Poll> {
        Ok(child.try_wait()?.map_or(Poll::Running, |status| {
            Poll::Collected(super::exit_of(status))
        }))
    }

    /// The processes descended from one, found through `/proc` by their parents.
    mod descendants {
        use alloc::vec::Vec;

        use rustix::process::Pid;

        /// Every process descended from `root`, each stopped so it starts no more, found again
        /// until no new one appears. Empty where there is no `/proc` to read.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        pub(super) fn stopped(root: Pid) -> Vec<Pid> {
            use alloc::collections::BTreeSet;

            use rustix::process::{Signal, kill_process};
            let mut found = BTreeSet::new();
            for _ in 0..16 {
                let parents = parents();
                let mut grown = false;
                let mut queue = Vec::from([root.as_raw_nonzero().get()]);
                while let Some(parent) = queue.pop() {
                    for (child, _) in parents.iter().filter(|(_, p)| **p == parent) {
                        if found.insert(*child) {
                            grown = true;
                            if let Some(pid) = Pid::from_raw(*child) {
                                let _ = kill_process(pid, Signal::STOP);
                            }
                        }
                        queue.push(*child);
                    }
                }
                if !grown {
                    break;
                }
            }
            found.into_iter().filter_map(Pid::from_raw).collect()
        }

        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        pub(super) fn stopped(_root: Pid) -> Vec<Pid> {
            Vec::new()
        }

        /// Each process's parent, as `/proc/<pid>/stat` says.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        fn parents() -> alloc::collections::BTreeMap<i32, i32> {
            let mut parents = alloc::collections::BTreeMap::new();
            let Ok(entries) = std::fs::read_dir("/proc") else {
                return parents;
            };
            for entry in entries.flatten() {
                let Some(pid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<i32>().ok())
                else {
                    continue;
                };
                let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
                    continue;
                };
                // `pid (name) state ppid ...`: the name may hold spaces and parentheses, so read
                // from its last `)`.
                let parent = stat
                    .rfind(')')
                    .and_then(|end| stat[end + 1..].split_whitespace().nth(1))
                    .and_then(|ppid| ppid.parse::<i32>().ok());
                if let Some(parent) = parent {
                    parents.insert(pid, parent);
                }
            }
            parents
        }
    }
}

#[cfg(windows)]
mod windows {
    use std::sync::Mutex;

    use super::{Child, Command, Poll, exit_of, io};
    use crate::sync::locked;

    /// A job object that holds the child and everything it starts and ends them all when its
    /// handle closes, which the operating system does too when this process ends.
    pub(in super::super) struct Tree(Mutex<Option<win32job::Job>>);

    /// Without a console of its own the child opens no window, whoever started this process.
    pub(in super::super) fn prepare(command: &mut Command, console: bool) {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        if !console {
            std::os::windows::process::CommandExt::creation_flags(command, CREATE_NO_WINDOW);
        }
    }

    /// Puts the child in a job of its own. A process it starts in the instant before it joins is
    /// outside the job.
    pub(in super::super) fn contain(child: &Child) -> io::Result<Tree> {
        use std::os::windows::io::AsRawHandle;
        let mut limits = win32job::ExtendedLimitInfo::new();
        limits.limit_kill_on_job_close();
        let job = win32job::Job::create_with_limit_info(&limits).map_err(io::Error::other)?;
        job.assign_process(child.as_raw_handle() as isize)
            .map_err(io::Error::other)?;
        Ok(Tree(Mutex::new(Some(job))))
    }

    /// Closing the job ends everything in it.
    pub(in super::super) fn end(tree: &Tree, _child: &Child) {
        drop(locked(&tree.0).take());
    }

    pub(in super::super) fn poll(child: &mut Child) -> io::Result<Poll> {
        Ok(child
            .try_wait()?
            .map_or(Poll::Running, |status| Poll::Collected(exit_of(status))))
    }
}
