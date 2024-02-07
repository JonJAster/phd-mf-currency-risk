using FromFile
using DataFrames
using CSV
using Arrow

# @from "../../utils.jl" include ProjectUtilities

function init_mf_data()
    mf_data_collection = read_mf_data()
    mf_data = merge_mf_data(mf_data_collection)
end

function read_mf_data()
    folder = "data/mutual-funds/raw"
    files = readdir(folder)
    data = Dict()
    for file in files
        data[file] = CSV.read(joinpath(folder, file), DataFrame)
    end
    return data
end

function merge_mf_data(data_collection)
    drop_

if abspath(PROGRAM_FILE) == @__FILE__
    init_mf_data()
end