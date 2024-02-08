using CSV
using DataFrames

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    folder = "data/mutual-funds/raw"
    files = readdir(folder)
    data = Dict()
    for file in files
        data[file] = CSV.read(joinpath(folder, file), DataFrame)
    end

    display(first(first(data)[2]))
    CommonFunctions._normalise_names!(first(data)[2])

    df_test = data[collect(keys(data))[2]]
    df_test2 = deepcopy(df_test)
    CommonFunctions._normalise_names!(df_test2)
    for i in [4, ncol(df_test)]
        println(names(df_test)[i])
    end

end