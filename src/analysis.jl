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
using Distributions
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function analysis()
    regout_usa_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=x->!(x.foreign));
    regout_usa_usa_totalret = regress_fund_flows(
        "ff_usa_ffc6"; filter_by=x->!(x.foreign), ret_type=:ret
    );
    regout_usa_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=x->!(x.foreign));
    regout_usa_dev_totalret = regress_fund_flows(
        "ff_dev_ffc6"; filter_by=x->!(x.foreign), ret_type=:ret
    );
    regout_wld_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=x->x.foreign);
    regout_wld_usa_totalret = regress_fund_flows(
        "ff_usa_ffc6"; filter_by=x->x.foreign, ret_type=:ret
    );
    regout_wld_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=x->x.foreign);
    regout_wld_dev_totalret = regress_fund_flows(
        "ff_dev_ffc6"; filter_by=x->x.foreign, ret_type=:ret
    );

    regout_usa_usa.summary
    regout_usa_dev.summary
    regout_wld_usa.summary
    regout_wld_dev.summary
    
    println(regout_usa_usa_totalret.summary)
    println(regout_usa_dev_totalret.summary)
    println(regout_wld_usa_totalret.summary)
    println(regout_wld_dev_totalret.summary)

    i_regout_usa_usa = _coef_idx(regout_usa_usa)
    v_usa_usa = vcov(regout_usa_usa.regfit)
    v_usa_usa_coefs = v_usa_usa[i_regout_usa_usa, i_regout_usa_usa]
    coef_usa_usa = coef(regout_usa_usa.regfit)[i_regout_usa_usa]

    i_regout_usa_dev = _coef_idx(regout_usa_dev)
    v_usa_dev = vcov(regout_usa_dev.regfit)
    v_usa_dev_coefs = v_usa_dev[i_regout_usa_dev, i_regout_usa_dev]
    coef_usa_dev = coef(regout_usa_dev.regfit)[i_regout_usa_dev]

    i_regout_wld_usa = _coef_idx(regout_wld_usa)
    v_wld_usa = vcov(regout_wld_usa.regfit)
    v_wld_usa_coefs = v_wld_usa[i_regout_wld_usa, i_regout_wld_usa]
    coef_wld_usa = coef(regout_wld_usa.regfit)[i_regout_wld_usa]

    i_regout_wld_dev = _coef_idx(regout_wld_dev)
    v_wld_dev = vcov(regout_wld_dev.regfit)
    v_wld_dev_coefs = v_wld_dev[i_regout_wld_dev, i_regout_wld_dev]
    coef_wld_dev = coef(regout_wld_dev.regfit)[i_regout_wld_dev]

    proportion_coef_usa_usa = _proportion_coefs(coef_usa_usa)
    proportion_coef_usa_dev = _proportion_coefs(coef_usa_dev)
    proportion_coef_wld_usa = _proportion_coefs(coef_wld_usa)
    proportion_coef_wld_dev = _proportion_coefs(coef_wld_dev)

    (proportion_coef_usa_dev - proportion_coef_usa_usa) .* 100
    (proportion_coef_wld_dev - proportion_coef_wld_usa) .* 100

    se_proportion_usa_usa = [
        _delta_se(i, coef_usa_usa, v_usa_usa_coefs) for i in 2:length(coef_usa_usa)
    ]
    se_proportion_usa_dev = [
        _delta_se(i, coef_usa_dev, v_usa_dev_coefs) for i in 2:length(coef_usa_dev)
    ]
    se_proportion_wld_usa = [
        _delta_se(i, coef_wld_usa, v_wld_usa_coefs) for i in 2:length(coef_wld_usa)
    ]
    se_proportion_wld_dev = [
        _delta_se(i, coef_wld_dev, v_wld_dev_coefs) for i in 2:length(coef_wld_dev)
    ]

    p_proportion_usa_usa = [
        2 * cdf(TDist(nrow(regout_usa_usa.regfit) - length(coef_usa_usa) - 1), -abs(proportion_coef_usa_usa[i] / se_proportion_usa_usa[i]))
        for i in 1:length(proportion_coef_usa_usa)
    ]

end
 
function _coef_idx(regout)
    findall(
        x->!isnothing(match(r"ret_.*", string(x.sym))),
        regout.regfit.mf.f.rhs.terms[2:end]
    )
end

_proportion_coefs(coefs) = coefs ./ coefs[1]
_deltagrad(coef, alpha_coef) = [-coef / alpha_coef^2, 1 / alpha_coef]

function _delta_se(i, coefs, v_coefs)
    delta_se = sqrt.(
        _deltagrad(coefs[i], coefs[1])' * v_coefs * _deltagrad.(coefs[i], coefs[1])
    )
    return delta_se
end



if isnothing(match(r"terminalserver.jl$", PROGRAM_FILE))
    analysis()
end