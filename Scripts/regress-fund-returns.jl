using DataFrames
using CSV
using GLM

include("CommonFunctions.jl")
using .CommonFunctions

const INPUT_FILESTRING_FUNDS = "./data/transformed/mutual-funds"

function main(options_folder)
    time_start = time()

end

if abspath(PROGRAM_FILE) == @__FILE__
    folderstring = option_foldername(currency_type="local")
    main(folderstring)
end