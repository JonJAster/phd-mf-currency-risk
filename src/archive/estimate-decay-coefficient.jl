using DataFrames
using Arrow

include("DataInit.jl")
using .DataInit



# AUTO TESTING
using CSV

# MANUAL TESTING
if false
    testdf = CSV.read("./data/transformed/mutual-funds/local-rets_eq-strict/mf_usa.csv", DataFrame)
end