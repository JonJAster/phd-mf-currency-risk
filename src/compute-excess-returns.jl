using DataFrames
using Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR_FUND = joinpath(DIRS.fund, "post-processing")
const INPUT_DIR_EQUITY = joinpath(DIRS.equity, "factor-series")

const OUTPUT_DIR = INPUT_DIR_FUND

function main(options_folder=option_foldername(DEFAULT_OPTIONS...))

end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end