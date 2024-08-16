using Revise
using DataFrames
using Arrow

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function bootstrapped_regressions()
    n_trials = 100#_000
    
    output_usa_ffc6 = _create_bootstrapped_table("ff_usa_ffc6", n_trials)
end

function _create_bootstrapped_table(model_name, n_trials)
    # model_name = "ff_usa_ffc6"; n_trials = 100
    task_start = time()

    true_regression = regress_fund_flows(model_name; bootstrapped=false)

end