use std::{
    io::{self, BufRead, IsTerminal, Write},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Interactive,
    Auto,
}

impl Mode {
    pub fn resolve(yes: bool) -> Self {
        if yes || !io::stdin().is_terminal() {
            Self::Auto
        } else {
            Self::Interactive
        }
    }
}

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


pub fn path(label: &str, default: Option<&str>, mode: Mode) -> Result<PathBuf> {
    let s = string(label, default, mode)?;
    Ok(PathBuf::from(s))
}

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
