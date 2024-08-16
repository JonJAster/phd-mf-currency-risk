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
    
    output_d = _create_bootstrapped_table(n_trials; filter_by=x->!x.foreign)
end

function _create_bootstrapped_table(n_trials; filter_by=nothing)
    # model_name = "ff_usa_ffc6"; n_trials = 100; filter_by=x->!x.foreign
    task_start = time()

    coefficient_table = DataFrame(
        :factor =>
            [:alpha, :wret_mkt, :wret_smb, :wret_hml, :wret_rmw, :wret_cma, :wret_wml],
        :usa_coef => Vector{Float64}(undef, 7),
        :dev_coef => Vector{Float64}(undef, 7),
        :usa_propα => Vector{Float64}(undef, 7),
        :dev_m_usa => Vector{Float64}(undef, 7),
        :dev_propα => Vector{Float64}(undef, 7)
    )

    true_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=false).summary
    true_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=false).summary

    bootstrapped_outputs = [copy(coefficient_table) for i in 1:n_trials]
    for i in bootstrapped_outputs
        # i = first(bootstrapped_outputs)
        boot_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        boot_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        _fill_coefficient_table!(i, boot_regression_usa, boot_regression_dev)
    end


end