using Revise
using DataFrames
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonFunctions
using .CommonConstants

function decompose_fund_returns(model_name) # model_name = "usa_capm_ver"
    task_start = time()

    model = MODELS[model_name]
    main_data = initialise_base_data(model)

    betas_filestring = joinpath(DIRS.combo.return_betas, "$model_name.arrow")
    regression_outputs = loadarrow(betas_filestring)

    betas = _extract_betas(regression_outputs)
    full_data = innerjoin(main_data, betas, on=[:fundid, :date])

    decomposed_returns = _decompose_returns(full_data, model)
    
    printtime("decomposing fund returns for $model_name", task_start)
    return decomposed_returns
end

_extract_betas(regression_outputs) = unstack(
    regression_outputs, [:fundid, :date], :factor, :coef, renamecols=f->Symbol("$(f)_beta")
)

function _decompose_returns(full_data, model)
    model_factors =  model[2]
    decomposed_returns = full_data[!, [:fundid, :date, :ex_ret]]
    decomposed_returns.ret_alpha .= 0.0

    for factor in model_factors # factor = first(model_factors)
        decomposed_returns[!, "ret_$(factor)"] = (
            full_data[!, factor] .* full_data[!, "$(factor)_beta"]
        )
    end

    decomposed_returns.ret_alpha = (
        full_data.ex_ret
        - sum(decomposed_returns[:, Symbol("ret_$(factor)")] for factor in model_factors)
    )

    decomposed_returns[.!ismissing.(decomposed_returns.ret_alpha), :]
    full_data[.!ismissing.(decomposed_returns.ret_alpha), :]
    dropmissing!(decomposed_returns)

    return decomposed_returns
end

function main()
    for model_name in ["usa_capm_ver"]
        # This loop is inefficient in that it reads the same data for each model, but is the
        # lowest effort way to enable calling the function for a single model from
        # elsewhere.
        output_data = decompose_fund_returns(model_name)
        output_filename = makepath(DIRS.combo.decomposed, "$model_name.arrow")

        Arrow.write(output_filename, output_data)
    end

    return
end

if abspath(PROGRAM_FILE) == @__FILE__
    task_start = time()
    main()
    printtime("decomposing all fund returns", task_start)
end