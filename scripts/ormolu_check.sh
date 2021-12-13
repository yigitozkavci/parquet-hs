if $(ormolu --mode check $(find . -name '*.hs')); then
    echo "parquet-hs is formatted correctly"
else
    echo "Please run ormolu"
fi
