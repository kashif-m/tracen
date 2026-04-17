use std::fs;

use tempfile::tempdir;
use tracen_pack::{build, PackBuildConfig};

#[test]
fn build_entry_supports_minimal_integration_layout() {
    let temp = tempdir().expect("tempdir");
    let dsl_path = temp.path().join("habit.tracker");
    fs::write(
        &dsl_path,
        r#"
tracker "habit" v1 {
  fields {
    day_bucket: int optional
  }
  metrics {
    total_events = count() over all_time
  }
  views {
    view "timeline" {
      config = {"query_type":"TimelineQuery","response_type":"TimelineResponse","result_kind":"metric_series","group_by":{"day":{"field":"day_bucket"}},"metrics":{"total_events":{"metric":"total_events","label":"Events"}}}
    }
  }
}
"#,
    )
    .expect("write dsl");

    let output = build(&PackBuildConfig {
        dsl_path,
        out_dir: temp.path().join("out"),
        generated_ts_dir: temp.path().join("generated"),
        base_source_paths: std::collections::BTreeMap::new(),
    })
    .expect("build pack");

    assert!(output.rust_artifact_path.exists());
    assert!(output.dsl_contract_path.exists());
    assert!(output.api_contract_path.exists());
    assert!(output.domain_contract_path.exists());
}

#[test]
fn build_supports_single_base_catalog_source() {
    let temp = tempdir().expect("tempdir");
    let dsl_path = temp.path().join("catalog.tracker");
    let catalog_path = temp.path().join("catalog.json");
    fs::write(
        &dsl_path,
        r#"
tracker "catalog" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "exercise" {
      base_source = "default_catalog"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
    )
    .expect("write dsl");
    fs::write(&catalog_path, r#"[{"slug":"bench_press"}]"#).expect("write catalog");

    let mut base_source_paths = std::collections::BTreeMap::new();
    base_source_paths.insert("default_catalog".to_string(), catalog_path);

    let output = build(&PackBuildConfig {
        dsl_path,
        out_dir: temp.path().join("out"),
        generated_ts_dir: temp.path().join("generated"),
        base_source_paths,
    })
    .expect("build pack");

    assert!(output.rust_artifact_path.exists());
}

#[test]
fn build_rejects_multiple_base_catalog_sources() {
    let temp = tempdir().expect("tempdir");
    let dsl_path = temp.path().join("catalog.tracker");
    fs::write(
        &dsl_path,
        r#"
tracker "catalog" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "exercise" {
      base_source = "default_catalog"
      fields = {"slug":{"type":"string"}}
    }
    entry "movement" {
      base_source = "secondary_catalog"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
    )
    .expect("write dsl");

    let error = build(&PackBuildConfig {
        dsl_path,
        out_dir: temp.path().join("out"),
        generated_ts_dir: temp.path().join("generated"),
        base_source_paths: std::collections::BTreeMap::new(),
    })
    .expect_err("multiple base sources should fail");

    assert!(error.to_string().contains("pack_base_catalog"));
    assert!(error.to_string().contains("default_catalog"));
    assert!(error.to_string().contains("secondary_catalog"));
}

#[test]
fn build_rejects_missing_declared_base_catalog_source_payload() {
    let temp = tempdir().expect("tempdir");
    let dsl_path = temp.path().join("catalog.tracker");
    fs::write(
        &dsl_path,
        r#"
tracker "catalog" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "exercise" {
      base_source = "default_catalog"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
    )
    .expect("write dsl");

    let error = build(&PackBuildConfig {
        dsl_path,
        out_dir: temp.path().join("out"),
        generated_ts_dir: temp.path().join("generated"),
        base_source_paths: std::collections::BTreeMap::new(),
    })
    .expect_err("missing base source should fail");

    assert!(error.to_string().contains("default_catalog"));
    assert!(error.to_string().contains("no payload was provided"));
}

#[test]
fn build_allows_duplicate_catalog_entries_that_share_one_base_source() {
    let temp = tempdir().expect("tempdir");
    let dsl_path = temp.path().join("catalog.tracker");
    let catalog_path = temp.path().join("catalog.json");
    fs::write(
        &dsl_path,
        r#"
tracker "catalog" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "exercise" {
      base_source = "default_catalog"
      fields = {"slug":{"type":"string"}}
    }
    entry "movement" {
      base_source = "default_catalog"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
    )
    .expect("write dsl");
    fs::write(&catalog_path, r#"[{"slug":"bench_press"}]"#).expect("write catalog");

    let mut base_source_paths = std::collections::BTreeMap::new();
    base_source_paths.insert("default_catalog".to_string(), catalog_path);

    let output = build(&PackBuildConfig {
        dsl_path,
        out_dir: temp.path().join("out"),
        generated_ts_dir: temp.path().join("generated"),
        base_source_paths,
    })
    .expect("duplicate shared source should succeed");

    assert!(output.rust_artifact_path.exists());
}
