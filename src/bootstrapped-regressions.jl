using DataFrames
using Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function bootstrapped_regressions()
    n_trials = 100_000
    MODELS
end