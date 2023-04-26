using DataFrames
using CSV

include("DataReader.jl")
using .DataReader

const INPUT_FILESTRING_BASE_BETAS = "./data/results"

function main(options_folder)
    time_start = time()

    betas_filestring = joinpath(INPUT_FILESTRING_BASE_BETAS, options_folder, "betas.csv")
    betas = CSV.read(betas_filestring, DataFrame)
    main_data = initialise_main_data(options_folder)

    full_data = innerjoin(main_data, betas, on=[:fundid, :date])

    println("Decomposing returns...")

    

if abspath(PROGRAM_FILE) == @__FILE__
    options_folder = option_foldername(currency_type="local")
    main(options_folder)
end