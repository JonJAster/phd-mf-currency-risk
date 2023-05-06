using DataFrames
using Arrow
using Base.Threads

include("CommonFunctions.jl")
include("CommonConstants.jl")
include("DataReader.jl")
using .CommonFunctions
using .CommonConstants
using .DataReader

const FILESTRING_BASE = "./data/results"

@inline extract_betas(regression_outputs) = unstack(
    regression_outputs, [:fundid, :date], :factor, :coef, renamecols=f->Symbol("$(f)_beta")
)

function decompose_returns(data, model)
    factors =  BENCHMARK_MODELS[model[2]] ∪ CURRENCYRISK_MODELS[model[1]]
    decomposed_returns = data[!, [:fundid, :date, :ret]]
    decomposed_returns.ret_alpha .= 0.0

    for factor in factors
        decomposed_returns[!, "ret_$(factor)"] = (
            data[!, factor] .* data[!, "$(factor)_beta"]
        )
    end

    decomposed_returns.ret_alpha = (
        data.ret - sum(decomposed_returns[:, Symbol("ret_$(factor)")] for factor in factors)
    )
    return decomposed_returns
end

function save_decomposition(decomposed_returns, output_folderstring, model_name, savelock)
    lock(savelock) do
        output_filestring = joinpath(output_folderstring, "$model_name.arrow")
        Arrow.write(output_filestring, decomposed_returns)
    end
end

function main(options_folder)
    time_start = time()

    main_data = initialise_main_data(options_folder)
    betas_folderstring = joinpath(FILESTRING_BASE, options_folder, "betas")
    output_folderstring = joinpath(FILESTRING_BASE, options_folder, "decompositions")

    if !isdir(output_folderstring)
        mkdir(output_folderstring)
    end

    savelock = ReentrantLock()
    @threads for model in COMPLETE_MODELS
        process_start = time()
        model_name = name_model(model)
        betas_filestring = joinpath(betas_folderstring, "$model_name.arrow")
        regression_outputs = Arrow.Table(betas_filestring) |> DataFrame

        betas = extract_betas(regression_outputs)
        full_data = innerjoin(main_data, betas, on=[:fundid, :date])

        decomposed_returns = decompose_returns(full_data, model)
        
        save_decomposition(decomposed_returns, output_folderstring, model_name, savelock)
        process_elapsed_s = round(time() - process_start, digits=2)
        process_elapsed_m = round(process_elapsed_s/60, digits=2)
        println(
            "Process finished decomposing returns on $(model_name) in " *
            "$process_elapsed_s seconds ($process_elapsed_m minutes)"
        )
    end

    time_duration_s = round(time() - time_start, digits=2)
    time_duration_m = round(time_duration_s/60, digits=2)
    println(
        "Finished decomposing fund returns in $time_duration_s seconds " *
        "($time_duration_m minutes)"
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    options_folder = option_foldername(currency_type="local", strict_eq=true)
    main(options_folder)
end