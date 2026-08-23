import ast
import copy
import importlib.util
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "retention_report.py"
VERIFY = ROOT / "scripts" / "verify-retention-deployment.sh"
SPEC = importlib.util.spec_from_file_location("retention_report", SCRIPT)
retention = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(retention)


class RetentionReportTests(unittest.TestCase):
    def make_config(self, base):
        checkpoint = base / "checkpoints"
        extra = base / "extra"
        recipes = base / "recipes"
        docs = base / "docs"
        reports = base / "reports"
        for path in (checkpoint, extra, recipes, docs, reports):
            path.mkdir()
        psql = base / "psql"
        psql.write_text("#!/bin/sh\nexit 1\n")
        psql.chmod(0o700)
        rules = {
            "scan": {
                "checkpoint_roots": [str(checkpoint)],
                "extra_group_dirs": [str(extra)],
                "owner": "owner",
                "colleagues": ["colleague"],
                "skip_namespaces": ["aliases"],
            },
            "registry": {
                "psql": str(psql),
                "pghost": str(base / "socket"),
                "pgport": "5432",
                "pgdatabase": "labctl",
            },
            "references": {
                "run_dir_marker": ".lab/submit.sh",
                "recipe_roots": [str(recipes)],
                "recipe_suffixes": [".toml"],
                "doc_roots": [str(docs)],
                "doc_suffixes": [".md"],
            },
            "lineage": {
                "crowdcast_markers": ["crowdcast"],
                "not_crowdcast_markers": ["finevision"],
                "no_signal_markers": ["base-model"],
                "max_depth": 8,
            },
            "policy": {
                "recent_days": 14,
                "name_crowdcast_pattern": "(?i)crowdcast",
                "enabled_rules": [
                    "not_ours",
                    "recent",
                    "referenced",
                    "crowdcast_mixture",
                    "name_only_crowdcast",
                    "crowdcast_pure_unreferenced",
                ],
            },
        }
        return rules, reports / "retention.md"

    @staticmethod
    def apply_rules(unit, order, now=2_000_000):
        context = {
            "colleagues": {"colleague"},
            "recent_days": 14,
            "name_re": re.compile("(?i)crowdcast"),
            "now": now,
        }
        for name in order:
            predicate, verdict, _ = retention.RULES[name]
            if predicate(unit, context):
                unit.decision = verdict
                unit.rule = name
                break

    @staticmethod
    def report_meta():
        return {
            "generated": "2026-08-23 00:00 UTC",
            "tool": str(SCRIPT),
            "owner": "owner",
            "colleagues": ["colleague"],
            "scan_roots": ["/checkpoints"],
            "recipe_roots": ["/recipes"],
            "submit_scanned": 0,
            "submit_missing": 0,
            "recipe_files": 1,
            "doc_files": 1,
            "doc_suffixes": [".md"],
        }

    def test_cli_exposes_only_read_only_inputs(self):
        tree = ast.parse(SCRIPT.read_text())
        options = {
            arg.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_argument"
            for arg in node.args
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
        }
        self.assertEqual(options, {"--rules", "--out"})
        for option in ("--apply", "--delete", "--force"):
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--rules",
                    "/missing.toml",
                    "--out",
                    "/missing.md",
                    option,
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unrecognized arguments", result.stderr)

    def test_subprocess_and_database_failures_raise(self):
        with mock.patch.object(retention.subprocess, "run", side_effect=OSError("exec")):
            with self.assertRaisesRegex(RuntimeError, "command failed"):
                retention.run_command("command", ["false"])
        with mock.patch.object(
            retention.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired(["slow"], 1),
        ):
            with self.assertRaisesRegex(RuntimeError, "command failed"):
                retention.run_command("command", ["slow"])
        failed = subprocess.CompletedProcess(["psql"], 2, b"", b"database unavailable")
        with mock.patch.object(retention.subprocess, "run", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "psql query failed"):
                retention.query(
                    {
                        "psql": "/psql",
                        "pghost": "/socket",
                        "pgport": "5432",
                        "pgdatabase": "labctl",
                    },
                    "SELECT 1",
                )

    def test_du_requires_complete_valid_output(self):
        cases = (b"", b"invalid\n", b"1\t2\t/other\n")
        for stdout in cases:
            with self.subTest(stdout=stdout):
                result = SimpleNamespace(stdout=stdout)
                with mock.patch.object(retention, "run_command", return_value=result):
                    with self.assertRaises(RuntimeError):
                        retention.du(["/unit"])

    def test_grep_failure_raises(self):
        failed = subprocess.CompletedProcess(["grep"], 2, b"", b"read error")
        with mock.patch.object(retention.subprocess, "run", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "reference grep failed"):
                retention.grep_fixed(["unit"], ["/doc"])

    def test_grep_preserves_colons_in_reference_paths(self):
        with tempfile.TemporaryDirectory() as raw:
            doc = Path(raw) / "handoff:notes.md"
            doc.write_text("unit-a\n")
            self.assertEqual(retention.grep_fixed(["unit-a"], [str(doc)]), [(str(doc), "unit-a")])

    def test_grep_scans_nul_containing_reference_files(self):
        with tempfile.TemporaryDirectory() as raw:
            doc = Path(raw) / "artifact.json"
            doc.write_bytes(b"\0unit-a\n")
            self.assertEqual(retention.grep_fixed(["unit-a"], [str(doc)]), [(str(doc), "unit-a")])

    def test_traversal_and_stat_failures_raise(self):
        def failed_walk(root, onerror):
            onerror(OSError("walk denied"))
            yield root, [], []

        with mock.patch.object(retention.os, "walk", side_effect=failed_walk):
            with self.assertRaisesRegex(OSError, "walk denied"):
                retention.collect_files(["/docs"], [".md"])
        with mock.patch.object(
            retention.os, "walk", return_value=[("/docs", [], ["reference.md"])]
        ), mock.patch.object(retention.os, "stat", side_effect=OSError("stat denied")):
            with self.assertRaisesRegex(OSError, "stat denied"):
                retention.collect_files(["/docs"], [".md"])

    def test_unknown_lineage_without_references_stays_keep(self):
        lineage = {
            "crowdcast_markers": ["crowdcast"],
            "not_crowdcast_markers": ["finevision"],
            "no_signal_markers": ["base-model"],
            "max_depth": 8,
        }
        units = [
            retention.Unit("/checkpoints/orphan", "owner", "orphan", 1, 0),
            retention.Unit("/checkpoints/unknown", "owner", "unknown", 1, 0),
        ]
        units[1].producers.add("run-a")
        retention.resolve_lineage(units[0], {}, lambda path: None, lineage)
        retention.resolve_lineage(
            units[1], {"run-a": {"/datasets/mystery"}}, lambda path: None, lineage
        )
        order = [
            "not_ours",
            "recent",
            "referenced",
            "crowdcast_mixture",
            "name_only_crowdcast",
            "crowdcast_pure_unreferenced",
        ]
        for unit in units:
            self.apply_rules(unit, order)
            self.assertEqual(unit.lineage_class, retention.UNKNOWN)
            self.assertEqual(unit.refs, [])
            self.assertEqual((unit.decision, unit.rule), ("KEEP", "default"))

    def test_protective_rules_win_before_delete_rules(self):
        order = [
            "not_ours",
            "recent",
            "referenced",
            "crowdcast_mixture",
            "name_only_crowdcast",
            "crowdcast_pure_unreferenced",
        ]
        recent = retention.Unit("/recent", "owner", "crowdcast", 1, 2_000_000)
        referenced = retention.Unit("/referenced", "owner", "crowdcast", 1, 0)
        colleague = retention.Unit("/colleague", "colleague", "crowdcast", 1, 0)
        for unit in (recent, referenced, colleague):
            unit.lineage_class = retention.CROWDCAST_PURE
        referenced.refs.append("(c) doc `/handoff.md`")
        for unit in (recent, referenced, colleague):
            self.apply_rules(unit, order)
        self.assertEqual((recent.decision, recent.rule), ("KEEP", "recent"))
        self.assertEqual((referenced.decision, referenced.rule), ("KEEP", "referenced"))
        self.assertEqual((colleague.decision, colleague.rule), ("NOT_OURS", "not_ours"))

    def test_config_rejects_duplicate_unknown_and_unprotected_policy(self):
        with tempfile.TemporaryDirectory() as raw:
            rules, out = self.make_config(Path(raw))
            duplicate = copy.deepcopy(rules)
            duplicate["scan"]["colleagues"].append("colleague")
            unknown = copy.deepcopy(rules)
            unknown["policy"]["enabled_rules"].append("guess")
            unprotected = copy.deepcopy(rules)
            moved = unprotected["policy"]["enabled_rules"].pop(1)
            unprotected["policy"]["enabled_rules"].insert(4, moved)
            for candidate in (duplicate, unknown, unprotected):
                with self.subTest(candidate=candidate):
                    with self.assertRaises(ValueError):
                        retention.validate_config(candidate, str(out))

    def test_scan_rejects_unknown_namespace_and_duplicate_unit_names(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root = base / "checkpoints"
            root.mkdir()
            scan = {
                "checkpoint_roots": [str(root)],
                "extra_group_dirs": [],
                "owner": "owner",
                "colleagues": ["colleague"],
                "skip_namespaces": ["aliases"],
            }
            (root / "stranger" / "unit").mkdir(parents=True)
            with self.assertRaisesRegex(RuntimeError, "unconfigured checkpoint namespace"):
                retention.scan_units(scan)
            shutil.rmtree(root / "stranger")
            (root / "owner" / "same-name").mkdir(parents=True)
            (root / "colleague" / "same-name").mkdir(parents=True)
            with self.assertRaisesRegex(RuntimeError, "globally unique"):
                retention.scan_units(scan)

    def test_output_rejects_estate_reference_symlinks_and_unrelated_files(self):
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            rules, out = self.make_config(base)
            estate = Path(rules["scan"]["checkpoint_roots"][0])
            with self.assertRaisesRegex(ValueError, "configured estate or reference root"):
                retention.validate_config(rules, str(estate / "report.md"))
            references = Path(rules["references"]["doc_roots"][0])
            with self.assertRaisesRegex(ValueError, "configured estate or reference root"):
                retention.validate_config(rules, str(references / "report.md"))
            linked_parent = base / "linked-estate"
            linked_parent.symlink_to(estate, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "configured estate or reference root"):
                retention.validate_config(rules, str(linked_parent / "report.md"))
            target = out.parent / "target.md"
            target.write_text("unrelated\n")
            link = out.parent / "linked.md"
            link.symlink_to(target)
            with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                retention.validate_config(rules, str(link))
            with self.assertRaisesRegex(ValueError, "not a retention report"):
                retention.validate_config(rules, str(target))

    def test_atomic_report_cleans_temp_on_success_and_failure(self):
        order = [
            "not_ours",
            "recent",
            "referenced",
            "crowdcast_mixture",
            "name_only_crowdcast",
            "crowdcast_pure_unreferenced",
        ]
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            output = parent / "report.md"
            retention.write_report(str(output), [], order, self.report_meta())
            self.assertTrue(output.read_text().startswith(retention.REPORT_HEADER))
            self.assertEqual(list(parent.glob(".retention_report.*")), [])
            failed_output = parent / "failed.md"
            with mock.patch.object(retention.os, "replace", side_effect=OSError("replace")):
                with self.assertRaisesRegex(OSError, "replace"):
                    retention.write_report(str(failed_output), [], order, self.report_meta())
            self.assertFalse(failed_output.exists())
            self.assertEqual(list(parent.glob(".retention_report.*")), [])

    def test_deployment_verifier_requires_exact_regular_files(self):
        with tempfile.TemporaryDirectory() as raw:
            deployed = Path(raw) / "tooling"
            deployed.mkdir()
            for name in ("retention_report.py", "retention_rules.toml"):
                shutil.copyfile(ROOT / "scripts" / name, deployed / name)
            exact = subprocess.run([str(VERIFY), str(deployed)], capture_output=True, text=True)
            self.assertEqual(exact.returncode, 0, exact.stderr)
            (deployed / "retention_rules.toml").write_text("changed\n")
            drift = subprocess.run([str(VERIFY), str(deployed)], capture_output=True, text=True)
            self.assertNotEqual(drift.returncode, 0)
            self.assertIn("differs from the repository", drift.stderr)
            (deployed / "retention_rules.toml").unlink()
            (deployed / "retention_rules.toml").symlink_to(ROOT / "scripts" / "retention_rules.toml")
            linked = subprocess.run([str(VERIFY), str(deployed)], capture_output=True, text=True)
            self.assertNotEqual(linked.returncode, 0)
            self.assertIn("missing a regular", linked.stderr)


if __name__ == "__main__":
    unittest.main()
