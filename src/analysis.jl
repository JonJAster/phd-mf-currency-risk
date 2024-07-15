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
    reg1 = regress_fund_flows("ff_usa_ffc6", filter_by=x->investment_target_is(x, :wld));
    r2(reg1.regfit)
    reg1.summary
    for row in 2:nrow(reg1.summary)
        println("$(round(reg1.summary[row, :coef]/reg1.summary[1, :coef],digits=4)*100)%")
    end
    reg2 = regress_fund_flows("ff_dev_ffc6", filter_by=x->investment_target_is(x, :wld));
    r2(reg2.regfit)
    reg2.summary[1,:coef] = 0.657
    for row in 2:nrow(reg2.summary)
        println("$(round(reg2.summary[row, :coef]/reg2.summary[1, :coef],digits=4)*100)%")
    end
    for row in 2:nrow(reg2.summary)
        println("$((round(reg2.summary[row, :coef]/reg2.summary[1, :coef],digits=4)*100)-(round(reg1.summary[row, :coef]/reg1.summary[1, :coef],digits=4)*100))%")
    end
    reg3 = regress_fund_flows("ff_usa_ffc6", filter_by=x->investment_target_is(x, :usa));
    r2(reg3.regfit)
    for row in 2:nrow(reg3.summary)
        println("$(round(reg3.summary[row, :coef]/reg3.summary[1, :coef],digits=4)*100)%")
    end
    reg4 = regress_fund_flows("ff_dev_ffc6", filter_by=x->investment_target_is(x, :usa))
    r2(reg4.regfit)
    reg4.summary[1,:coef] = 1.192
    for row in 2:nrow(reg4.summary)
        println("$(round(reg4.summary[row, :coef]/reg4.summary[1, :coef],digits=4)*100)%")
    end
    for row in 2:nrow(reg4.summary)
        println("$((round(reg4.summary[row, :coef]/reg4.summary[1, :coef],digits=4)*100)-(round(reg3.summary[row, :coef]/reg3.summary[1, :coef],digits=4)*100))%")
    end
    regress_fund_flows("ff_usa_ffc6")
    regress_fund_flows("ff_dev_ffc6")
    
    regress_fund_flows("ff_usa_ffc4", filter_by=x->investment_target_is(x, :wld))
    regress_fund_flows("ff_dev_ffc", filter_by=x->investment_target_is(x, :wld))
    regress_fund_flows("ff_usa_ffc", filter_by=x->investment_target_is(x, :usa))
    regress_fund_flows("ff_dev_ffc", filter_by=x->investment_target_is(x, :usa))
    regress_fund_flows("ff_usa_ffc")
    regress_fund_flows("ff_dev_ffc")

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