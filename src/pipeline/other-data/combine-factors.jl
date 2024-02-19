using Revise
using DataFrames
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function combine_factors()
    task_start = time()

    lms_filename = joinpath(DIRS.eq.factors, "lms.arrow")
    mkt_filename = joinpath(DIRS.eq.factors, "mkt.arrow")
    fx_filename = joinpath(DIRS.fx.factors, "currency_factors.arrow")

    lms_data = Arrow.Table(lms_filename) |> DataFrame
    mkt_data = Arrow.Table(mkt_filename) |> DataFrame
    fx_data = Arrow.Table(fx_filename) |> DataFrame

    _prep_mkt!(mkt_data)
    _prep_fx!(fx_data)

    equity_factors = vcat(lms_data, mkt_data, fx_data)

    printtime("combining factors", task_start)
    return equity_factors
end

function _prep_mkt!(mkt_data)
    rename!(mkt_data, :mkt_exc => :ret)
    mkt_data.factor .= "mkt"
    return mkt_data
end

function _prep_fx!(fx_data)
    fx_data.region .= "FX"
    return fx_data
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = combine_factors()
    output_filename = makepath(DIRS.combo.factors, "factors.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing combined factors", task_start)
end