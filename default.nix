{ compiler ? "ghc901" }:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs { };

  inherit (pkgs.haskell.lib) dontCheck;

  baseHaskellPkgs = pkgs.haskell.packages.${compiler};

  myHaskellPackages = baseHaskellPkgs.override {
    overrides = self: super: {
      parquet-hs = self.callCabal2nix "parquet-hs" (./.) { };
      # I opened up an MR fixing `pinch` on nixpkgs by bumping the version of
      # network `pinch` uses but it's causing issues here so instead we're
      # just disabling the test suite here as that's where the error is.
      pinch = dontCheck super.pinch;
    };
  };

  shell = myHaskellPackages.shellFor {
    packages = p: with p; [ parquet-hs ];

    buildInputs = with pkgs.haskellPackages; [
      cabal-install
      ghcid
      ormolu
      hlint
      pkgs.niv
      pkgs.nixpkgs-fmt
    ];

    libraryHaskellDepends = [ ];

    shellHook = ''
      set -e
      hpack
      set +e
    '';
  };

in {
  inherit shell;
  inherit myHaskellPackages;
  parquet-hs = myHaskellPackages.parquet-hs;
}
