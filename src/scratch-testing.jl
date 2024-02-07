using CSV
using DataFrames

function test()
    folder = "data/mutual-funds/raw"
    files = readdir(folder)
    data = Dict()
    for file in files
        data[file] = CSV.read(joinpath(folder, file), DataFrame)
    end
    
    k = keys(data) |> collect
    display(data[k[1]])
    display(data[k[2]])
end