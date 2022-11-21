with (import <nixpkgs> {});
mkShell {
  buildInputs = [
    python39Full
    jdk11  
  ];
  shellHook = ''source env/bin/activate'';
}
