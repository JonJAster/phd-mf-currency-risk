using Revise
using DataFrames
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function combine_factors()
    task_start = time()

    jkp_lms_filename = joinpath(DIRS.eq.factors, "jkp-lms.arrow")
    jkp_mkt_filename = joinpath(DIRS.eq.factors, "jkp-mkt.arrow")
    ff_filename = joinpath(DIRS.eq.factors, "ff.arrow")
    fx_filename = joinpath(DIRS.fx.factors, "currency_factors.arrow")

    jkp_lms_data = loadarrow(jkp_lms_filename)
    jkp_mkt_data = loadarrow(jkp_mkt_filename)
    ff_data = loadarrow(ff_filename)
    fx_data = loadarrow(fx_filename)

    _prep_fx!(fx_data)

    equity_factors = vcat(jkp_lms_data, jkp_mkt_data, ff_data, fx_data)

    printtime("combining factors", task_start)
    return equity_factors
end

function _prep_fx!(fx_data)
    fx_data.source_id .= "fx"
    return fx_data
end

if !isnothing(match(r"terminalserver.jl", PROGRAM_FILE))
    output_data = combine_factors()
    output_filename = makepath(DIRS.combo.factors, "factors.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing combined factors", task_start)
end