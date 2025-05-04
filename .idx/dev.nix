{
  packages = with import <nixpkgs> {}; [
    python311
    openjdk11
    python311Packages.pip
    python311Packages.setuptools
    python311Packages.wheel
    python311Packages.ipykernel
    python311Packages.pyspark
    python311Packages.jupyterlab
    python311Packages.notebook
    python311Packages.pandas
    python311Packages.matplotlib
  ];

  idx.extensions = [
    "ms-python.debugpy"
    "ms-python.python"
    "ms-toolsai.jupyter"
    "ms-toolsai.jupyter-keymap"
    "ms-toolsai.jupyter-renderers"
    "ms-toolsai.vscode-jupyter-cell-tags"
    "ms-toolsai.vscode-jupyter-slideshow"
  ];
}
