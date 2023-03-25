using CSV
using DataFrames

const FILESTRING = "./Data/Refined Data/Currencies/currency_rates.csv"

function main()
    df = CSV.read(FILESTRING, DataFrame)
    println(first(df,5))
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end