using Revise
using DataFrames
using CSV
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function analysis()
    regress_fund_flows("ff_usa_capm", filter_by=x->investment_target_is(x, :usa))
    regress_fund_flows("jkp_usa_capm", filter_by=x->investment_target_is(x, :usa))

    regress_fund_flows("ff_usa_ff3", filter_by=x->investment_target_is(x, :usa))
    regress_fund_flows("jkp_usa_ff3", filter_by=x->investment_target_is(x, :usa))

    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :usa) .&& bho_dates_only(x)))
    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :usa) .&& post_bho_only(x)))

    regress_fund_flows("ff_usa_ff3", filter_by=x->(investment_target_is(x, :usa) .&& bho_dates_only(x)))
    regress_fund_flows("ff_usa_ff3", filter_by=x->(investment_target_is(x, :usa) .&& post_bho_only(x)))

    regress_fund_flows("ff_usa_ff3_ver", filter_by=x->(investment_target_is(x, :usa) .&& bho_dates_only(x)))
    regress_fund_flows("ff_usa_ff3_ver", filter_by=x->(investment_target_is(x, :usa) .&& post_bho_only(x)))
    regress_fund_flows("ff_usa_ff3_ver", filter_by=x->investment_target_is(x, :usa))

    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :wld)))
    regress_fund_flows("ff_usa_ffc6_ver", filter_by=x->(investment_target_is(x, :wld)))
    regress_fund_flows("ff_dev_ffc6", filter_by=x->(investment_target_is(x, :wld)))
    regress_fund_flows("ff_dev_ffc6_ver", filter_by=x->(investment_target_is(x, :wld)))

    regress_fund_flows("ff_usa_ffc6", filter_by=x->(investment_target_is(x, :usa)))
end

if abspath(PROGRAM_FILE) == @__FILE__
    analysis()
end