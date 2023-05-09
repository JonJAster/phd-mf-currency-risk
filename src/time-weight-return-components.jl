using
    DataFrames,
    Arrow,
    Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")

using
    .CommonConstants,
    .CommonFunctions

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const OUTPUT_DIR = INPUT_DIR

function main(options_folder)
    for model in COMPLETE_MODELS
        