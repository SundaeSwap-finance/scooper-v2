/*
based on
https://discourse.nixos.org/t/how-can-i-set-up-my-rust-programming-environment/4501/9
*/
let
  rust_overlay = import (builtins.fetchTarball "https://github.com/oxalica/rust-overlay/archive/master.tar.gz");
  pkgs = import <nixpkgs> { overlays = [ rust_overlay ]; };
  rustVersion = "2025-06-10"; # "1.88.0";
  rust = pkgs.rust-bin.nightly.${rustVersion}.default.override {
    extensions = [
      "rust-src" # for rust-analyzer
      "rust-analyzer"
    ];
  };
in
pkgs.mkShell.override {
  stdenv = pkgs.clang12Stdenv;
} {
  buildInputs = [
    rust
    pkgs.cargo-dist
    pkgs.rustup
  ] ++ (with pkgs; [
    pkg-config
    # other dependencies
    #gtk3
    #wrapGAppsHook
  ]);
  RUST_BACKTRACE = 1;
  LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
}
