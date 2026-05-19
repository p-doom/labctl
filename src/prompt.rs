//! Tiny no-dep prompt helper for `labctl init` and any future
//! interactive subcommands. Three primitives — text-with-default,
//! single-choice menu, yes/no — all of which collapse to "use the
//! default" when stdin isn't a TTY or when the caller asks for
//! non-interactive (`--yes`) mode. That collapse is what makes a
//! single code path serve both interactive humans and scripted CI.
//!
//! Non-TTY detection uses std's `IsTerminal` (stable since 1.70). On
//! read-EOF or detected-non-TTY we return the supplied default;
//! callers that REQUIRE a value (no good default) pass `default =
//! None` and get an error in non-interactive mode, meant to surface
//! to the user as "pass `--<flag>` or run in a terminal."

use std::{
    io::{self, BufRead, IsTerminal, Write},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};

/// Mode toggles interactive vs auto-accept-defaults behavior.
/// `Auto` is set by `--yes`, by non-TTY stdin, or when the caller
/// has decided no human is at the keyboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Interactive,
    Auto,
}

impl Mode {
    /// Resolve the effective mode: honor an explicit `--yes` request,
    /// otherwise check whether stdin is a TTY (piped or scripted →
    /// auto).
    pub fn resolve(yes: bool) -> Self {
        if yes || !io::stdin().is_terminal() {
            Self::Auto
        } else {
            Self::Interactive
        }
    }
}

/// Prompt for a free-form string. `default` is shown in brackets and
/// returned on empty input. Returns an error only on I/O failure or
/// when there's no default in `Auto` mode.
pub fn string(label: &str, default: Option<&str>, mode: Mode) -> Result<String> {
    if mode == Mode::Auto {
        return default
            .map(|s| s.to_string())
            .with_context(|| format!("{label}: no default and running non-interactively"));
    }
    loop {
        match default {
            Some(d) => print!("  {label} [{d}]: "),
            None => print!("  {label}: "),
        }
        io::stdout().flush().ok();
        let mut line = String::new();
        let n = io::stdin().lock().read_line(&mut line)?;
        if n == 0 {
            // EOF mid-prompt — treat as accept-default (or bail if
            // there isn't one). Without this, the loop would spin
            // forever on empty reads after the terminal disconnects.
            return default
                .map(|s| s.to_string())
                .context("stdin closed mid-prompt and no default supplied");
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if let Some(d) = default {
                return Ok(d.to_string());
            }
            println!("    (required — please enter a value)");
            continue;
        }
        return Ok(trimmed.to_string());
    }
}

/// Convenience wrapper for path inputs.
pub fn path(label: &str, default: Option<&str>, mode: Mode) -> Result<PathBuf> {
    let s = string(label, default, mode)?;
    Ok(PathBuf::from(s))
}

/// Yes/no prompt. `default = true` → `[Y/n]`; `false` → `[y/N]`.
/// Re-prompts on unrecognized input rather than guessing.
pub fn confirm(label: &str, default: bool, mode: Mode) -> Result<bool> {
    if mode == Mode::Auto {
        return Ok(default);
    }
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    loop {
        print!("  {label} {suffix}: ");
        io::stdout().flush().ok();
        let mut line = String::new();
        let n = io::stdin().lock().read_line(&mut line)?;
        if n == 0 {
            return Ok(default);
        }
        match line.trim() {
            "" => return Ok(default),
            s if s.eq_ignore_ascii_case("y") || s.eq_ignore_ascii_case("yes") => {
                return Ok(true);
            }
            s if s.eq_ignore_ascii_case("n") || s.eq_ignore_ascii_case("no") => {
                return Ok(false);
            }
            _ => println!("    (please answer y or n)"),
        }
    }
}

/// Single-choice menu with `default_idx` (0-based) marked. Returns
/// the chosen 0-based index.
pub fn choice(label: &str, options: &[&str], default_idx: usize, mode: Mode) -> Result<usize> {
    if default_idx >= options.len() {
        bail!("invalid default index for {label}");
    }
    if mode == Mode::Auto {
        return Ok(default_idx);
    }
    println!("  {label}:");
    for (i, opt) in options.iter().enumerate() {
        let marker = if i == default_idx { "*" } else { " " };
        println!("    {marker} [{}] {opt}", i + 1);
    }
    loop {
        print!("  choice [{}]: ", default_idx + 1);
        io::stdout().flush().ok();
        let mut line = String::new();
        let n = io::stdin().lock().read_line(&mut line)?;
        if n == 0 {
            return Ok(default_idx);
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(default_idx);
        }
        match trimmed.parse::<usize>() {
            Ok(n) if n >= 1 && n <= options.len() => return Ok(n - 1),
            _ => println!("    (enter a number 1..{})", options.len()),
        }
    }
}
