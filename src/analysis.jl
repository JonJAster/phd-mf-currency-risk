






    
    
    # Excluding vs Including Currency Factors - Developed Factors on Intl Funds
    # Excluding vs Including Currency Factors - Meaningless Control: USA Factors on USA Funds
    # Pre vs Post BHO - FF3
    # Pre vs Post BHO - FFC6
    analysis()
    regress_fund_flows("ff_dev_ff3_ver", filter_by=x->investment_target_is(x, :wld))
    regress_fund_flows("ff_dev_ff3", filter_by=x->investment_target_is(x, :wld))
    regress_fund_flows("ff_dev_ffc6_ver", filter_by=x->(investment_target_is(x, :wld)))
    regress_fund_flows("ff_dev_ffc6", filter_by=x->(investment_target_is(x, :wld)))
    regress_fund_flows("ff_usa_ff3_ver", filter_by=x->investment_target_is(x, :usa))
    regress_fund_flows("ff_usa_ff3", filter_by=x->(investment_target_is(x, :usa) .&& bho_dates_only(x)))
    regress_fund_flows("ff_usa_ff3", filter_by=x->(investment_target_is(x, :usa) .&& post_bho_only(x)))
    regress_fund_flows("ff_usa_ff3", filter_by=x->investment_target_is(x, :usa))
    regress_fund_flows("ff_usa_ffc6_ver", filter_by=x->(investment_target_is(x, :wld)))
    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :usa) .&& bho_dates_only(x)))
    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :usa) .&& post_bho_only(x)))
    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :usa)))
    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :wld)))
end
end
function analysis()
if abspath(PROGRAM_FILE) == @__FILE__
includet("pipeline/regressions/regress-fund-flows.jl")
includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
using .CommonConstants
using .CommonFunctions
using .RegressFundFlows
using Arrow
using Base.Threads
using CSV
using DataFrames
using DataStructures
using Dates
using GLM
using LinearAlgebra
using Plots
using Revise
using ShiftedArrays: lead, lag
using StatsBase