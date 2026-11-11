use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;

/// What to start: a program, its arguments, its working directory and its environment.
///
/// The program is run directly, never through a shell, and each argument reaches it as one
/// argument. Nothing is inherited beyond the environment, the working directory and the standard
/// streams the process is given. `Debug` names the environment's variables and never their values,
/// which may be credentials.
#[derive(Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct Launch {
    program: OsString,
    args: Vec<OsString>,
    cwd: Option<PathBuf>,
    env: BTreeMap<OsString, OsString>,
    remove_env: Vec<OsString>,
    clear_env: bool,
}

impl Launch {
    /// A launch of `program` with no arguments, in this process's directory and environment. The
    /// program is found on `PATH` when it names no directory.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
            ..Self::default()
        }
    }

    /// Adds one argument.
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    /// Adds arguments, in order.
    pub fn args(mut self, args: impl IntoIterator<Item = impl AsRef<OsStr>>) -> Self {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self
    }

    /// Sets the working directory.
    pub fn cwd(mut self, cwd: impl AsRef<Path>) -> Self {
        self.cwd = Some(cwd.as_ref().to_owned());
        self
    }

    /// Sets one variable over the inherited environment.
    pub fn env(mut self, name: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.env
            .insert(name.as_ref().to_owned(), value.as_ref().to_owned());
        self
    }

    /// Removes one variable from the inherited environment.
    pub fn remove_env(mut self, name: impl AsRef<OsStr>) -> Self {
        self.remove_env.push(name.as_ref().to_owned());
        self
    }

    /// Starts from an empty environment instead of this process's; variables set with
    /// [`env`](Self::env) are still set.
    pub const fn clear_env(mut self, clear: bool) -> Self {
        self.clear_env = clear;
        self
    }

    /// The program.
    #[must_use]
    pub fn program(&self) -> &OsStr {
        &self.program
    }

    /// The arguments, in order.
    #[must_use]
    pub fn arguments(&self) -> &[OsString] {
        &self.args
    }

    /// The command with everything but its standard streams and its place in a process group.
    pub(super) fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        if self.clear_env {
            command.env_clear();
        }
        for name in &self.remove_env {
            command.env_remove(name);
        }
        command.envs(&self.env);
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        command
    }
}

/// The command line as one text, for messages: the program and each argument, quoted when it holds a
/// space or is empty. Not a shell line: nothing in it is escaped for one.
impl fmt::Display for Launch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&quoted(&self.program))?;
        for arg in &self.args {
            write!(f, " {}", quoted(arg))?;
        }
        Ok(())
    }
}

fn quoted(word: &OsStr) -> String {
    let word = word.to_string_lossy();
    if word.is_empty() || word.contains(' ') {
        alloc::format!("\"{word}\"")
    } else {
        word.into_owned()
    }
}

impl fmt::Debug for Launch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launch")
            .field("program", &self.program)
            .field("args", &self.args.len())
            .field("cwd", &self.cwd)
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .field("remove_env", &self.remove_env)
            .field("clear_env", &self.clear_env)
            .finish()
    }
}
