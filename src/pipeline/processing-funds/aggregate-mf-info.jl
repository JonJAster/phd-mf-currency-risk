using FromFile
using DataFrames
using Arrow

@from "../../utils.jl" using ProjectUtilities

const UNIQUE_AGG_COLS = [
    "domicile", "base_currency", "global_broad_category_group",
    "management_approach_active", "management_approach_passive", "true_no_load"
]

const RENAME_MAP = Dict(
    "base_currency" => "currency", "global_broad_category_group" => "broad_category",
    "management_approach_active" => "active", "management_approach_passive" => "passive",
    "true_no_load" => "no_load"
)

const ISO_MAP = Dict(
    "Andorra" => "AND", "Argentina" => "ARG", "Australia" => "AUS", "Austria" => "AUT",
    "Bahamas" => "BHS", "Bahrain" => "BHR", "Barbados" => "BRB", "Belgium" => "BEL",
    "Bermuda" => "BMU", "Botswana" => "BWA", "Brazil" => "BRA", 
    "British Virgin Islands" => "VGB", "Bulgaria" => "BGR", "Canada" => "CAN", 
    "Cayman Islands" => "CYM", "Chile" => "CHL", "China" => "CHN", "Colombia" => "COL", 
    "Curaçao" => "CUW", "Cyprus" => "CYP", "Czech Republic" => "CZE", "Denmark" => "DNK",
    "Estonia" => "EST", "Finland" => "FIN", "France" => "FRA", "Germany" => "DEU", 
    "Gibraltar" => "GIB", "Greece" => "GRC", "Guernsey" => "GGY", "Hong Kong" => "HKG", 
    "Hungary" => "HUN", "Iceland" => "ISL", "India" => "IND", "Indonesia" => "IDN", 
    "Ireland" => "IRL", "Isle of Man" => "IMN", "Israel" => "ISR", "Italy" => "ITA", 
    "Japan" => "JPN", "Jersey" => "JEY", "Jordan" => "JOR", "Kuwait" => "KWT", 
    "Latvia" => "LVA", "Lebanon" => "LBN", "Lesotho" => "LSO", "Liechtenstein" => "LIE", 
    "Lithuania" => "LTU", "Luxembourg" => "LUX", "Malaysia" => "MYS", "Malta" => "MLT", 
    "Marshall Islands" => "MHL", "Mauritius" => "MUS", "Mexico" => "MEX", "Monaco" => "MCO", 
    "Namibia" => "NAM", "Netherlands" => "NLD", "Netherlands Antilles" => "ANT", 
    "New Zealand" => "NZL", "Norway" => "NOR", "Oman" => "OMN", "Pakistan" => "PAK", 
    "Panama" => "PAN", "Peru" => "PER", "Philippines" => "PHL", "Poland" => "POL", 
    "Portugal" => "PRT", "Puerto Rico" => "PRI", "Qatar" => "QAT", 
    "Russian Federation" => "RUS", "Samoa" => "WSM", "San Marino" => "SMR", 
    "Saudi Arabia" => "SAU", "Singapore" => "SGP", "Slovenia" => "SVN", 
    "South Africa" => "ZAF", "South Korea" => "KOR", "Spain" => "ESP",
    "St Vincent-Grenadines" => "VCT", "Swaziland" => "SWZ", "Sweden" => "SWE",
    "Switzerland" => "CHE", "Taiwan" => "TWN", "Thailand" => "THA", "Tunisia" => "TUN", 
    "Turkey" => "TUR", "US Virgin Islands" => "VIR", "Uganda" => "UGA", "Ukraine" => "UKR", 
    "United Arab Emirates" => "ARE", "United Kingdom" => "GBR", "United States" => "USA",
    "Uruguay" => "URY", "Vanuatu" => "VUT", "Viet Nam" => "VNM"
)

function main()
    startedat = time()
    println("Reading data...")
    fundsec_info = qload(PATHS.groupedfunds, "info")
    dropmissing!(fundsec_info, :fundid)

    println("Aggregating to fund level...")
    fund_info = groupby(fundsec_info, :fundid) |> gb->combine(
        gb, UNIQUE_AGG_COLS .=> agg_if_unique, "inception_date" => minimum; renamecols=false
    )

    fund_info.domicile = map(country -> ISO_MAP[country], fund_info.domicile)
    rename!(fund_info, RENAME_MAP)

    output_filestring = makepath(OUTPUT_DIR, "mf_info.arrow")
    Arrow.write(output_filestring, fund_info)

    printtime("aggregating info to the fund level", startedat)
end

function testdropmissing!(df, col)
    incomplete_mask = any(ismissing.(df[!, col]), dims=2) |> vec
    incomplete_rows = (1:nrow(df))[incomplete_mask]
    deleteat!(df, incomplete_rows)
end

agg_if_unique(v) = length(unique(v)) == 1 ? first(v) : missing

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

dftest = Arrow.Table(joinpath(DIRS.fund, "post-processing", option_foldername(;DEFAULT_OPTIONS...), "main/fund_data.arrow")) |> DataFrame