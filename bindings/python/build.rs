fn main() {
    pyo3_build_config::use_pyo3_cfgs();
    println!("cargo::rustc-check-cfg=cfg(allocator, values(\"default\", \"mimalloc\"))");
}
