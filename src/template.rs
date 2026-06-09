use std::{collections::BTreeMap, path::Path};

use anyhow::{Result, bail};
use serde_json::Value;

use crate::store::InputResolution;

#[derive(Debug, Clone)]
pub struct RenderContext<'a> {
    pub run_id: &'a str,
    pub run_dir: &'a Path,
    pub recipe_name: &'a str,
    pub params: &'a BTreeMap<String, Value>,
    pub args: &'a BTreeMap<String, String>,
    pub inputs: &'a [InputResolution],
    pub outputs: &'a BTreeMap<String, std::path::PathBuf>,
}

pub fn render_value(template: &str, ctx: &RenderContext<'_>) -> Result<String> {
    let mut out = template.to_string();
    out = out.replace("{run.id}", ctx.run_id);
    out = out.replace("{run.dir}", &ctx.run_dir.display().to_string());
    out = out.replace("{recipe.name}", ctx.recipe_name);

    for (key, value) in ctx.params {
        out = out.replace(&format!("{{params.{key}}}"), &scalar_to_string(value));
    }
    // `[args]` table values, exposed for self-reference (e.g. a derived
    // cache_dir built from {args.model_id}_{args.max_length}). Single-pass,
    // like params — an arg value that itself contains another {args.*} token
    // resolves only if that token's value is already literal.
    for (key, value) in ctx.args {
        out = out.replace(&format!("{{args.{key}}}"), value);
    }
    for input in ctx.inputs {
        out = out.replace(
            &format!("{{inputs.{}.path}}", input.role),
            &input.resolved_path.display().to_string(),
        );
        if let Some(artifact_id) = &input.artifact_id {
            out = out.replace(&format!("{{inputs.{}.id}}", input.role), artifact_id);
        }
    }
    for (role, path) in ctx.outputs {
        out = out.replace(
            &format!("{{outputs.{role}.path}}"),
            &path.display().to_string(),
        );
    }
    if let Some(token) = find_unresolved_token(&out) {
        bail!("unresolved template token {token:?} in {template:?} -> {out:?}");
    }
    Ok(out)
}

/// Detect a labctl-style template token (``{word(.word)*}``) that survived
/// substitution. We deliberately only match the labctl token shape — bare
/// ``{`` and ``}`` characters from inline JSON or other content are
/// allowed through. Returns the first such token, or None.
fn find_unresolved_token(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'{' {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        while j < bytes.len() {
            let c = bytes[j];
            if c.is_ascii_alphanumeric() || c == b'.' || c == b'_' {
                j += 1;
            } else {
                break;
            }
        }
        if j > i + 1 && j < bytes.len() && bytes[j] == b'}' {
            return Some(s[i..=j].to_string());
        }
        i += 1;
    }
    None
}

pub fn scalar_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn ctx<'a>(
        recipe_name: &'a str,
        args: &'a BTreeMap<String, String>,
        params: &'a BTreeMap<String, Value>,
        outputs: &'a BTreeMap<String, std::path::PathBuf>,
        inputs: &'a [InputResolution],
    ) -> RenderContext<'a> {
        RenderContext {
            run_id: "run_abc",
            run_dir: Path::new("/runs/run_abc"),
            recipe_name,
            params,
            args,
            inputs,
            outputs,
        }
    }

    #[test]
    fn renders_recipe_name_token() {
        let (args, params, outputs) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
        let c = ctx("lora_qwen3vl2b_128k", &args, &params, &outputs, &[]);
        assert_eq!(
            render_value("{recipe.name}_{run.id}", &c).unwrap(),
            "lora_qwen3vl2b_128k_run_abc"
        );
    }

    #[test]
    fn renders_args_self_reference() {
        let mut args = BTreeMap::new();
        args.insert("model_id".into(), "Qwen/Qwen3-VL-2B-Instruct".into());
        args.insert("max_length".into(), "131072".into());
        let (params, outputs) = (BTreeMap::new(), BTreeMap::new());
        let c = ctx("r", &args, &params, &outputs, &[]);
        assert_eq!(
            render_value("cache/{args.model_id}_len_{args.max_length}", &c).unwrap(),
            "cache/Qwen/Qwen3-VL-2B-Instruct_len_131072"
        );
    }

    #[test]
    fn unknown_token_still_errors() {
        let (args, params, outputs) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
        let c = ctx("r", &args, &params, &outputs, &[]);
        assert!(render_value("{args.does_not_exist}", &c).is_err());
        assert!(render_value("{recipe.unknown}", &c).is_err());
    }
}
