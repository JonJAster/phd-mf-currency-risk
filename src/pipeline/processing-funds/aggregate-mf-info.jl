using
    DataFrames,
    Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "domicile-grouped/info")
const OUTPUT_DIR = joinpath(DIRS.fund, "info")

const UNIQUE_AGG_COLS = [
    "domicile", "base-currency", "global-broad-category-group",
    "management-approach---active", "management-approach---passive", "true-no-load"
]

const RENAME_MAP = Dict(
    "base-currency" => "currency", "global-broad-category-group" => "broad_category",
    "management-approach---active" => "active", "management-approach---passive" => "passive",
    "true-no-load" => "no_load", "inception-date" => "inception_date"
)

const ISO_MAP = Dict(
    "Andorra" => "AND", "Argentina" => "ARG", "Australia" => "AUS", "Austria" => "AUT",
    "Bahamas" => "BHS", "Bahrain" => "BHR", "Barbados" => "BRB", "Belgium" => "BEL",
    "Bermuda" => "BMU", "Botswana" => "BWA", "Brazil" => "BRA", "British Virgin Islands" => "VGB",
    "Bulgaria" => "BGR", "Canada" => "CAN", "Cayman Islands" => "CYM", "Chile" => "CHL",
    "China" => "CHN", "Colombia" => "COL", "Curaçao" => "CUW", "Cyprus" => "CYP",
    "Czech Republic" => "CZE", "Denmark" => "DNK", "Estonia" => "EST", "Finland" => "FIN",
    "France" => "FRA", "Germany" => "DEU", "Gibraltar" => "GIB", "Greece" => "GRC",
    "Guernsey" => "GGY", "Hong Kong" => "HKG", "Hungary" => "HUN", "Iceland" => "ISL",
    "India" => "IND", "Indonesia" => "IDN", "Ireland" => "IRL", "Isle of Man" => "IMN",
    "Israel" => "ISR", "Italy" => "ITA", "Japan" => "JPN", "Jersey" => "JEY",
    "Jordan" => "JOR", "Kuwait" => "KWT", "Latvia" => "LVA", "Lebanon" => "LBN",
    "Lesotho" => "LSO", "Liechtenstein" => "LIE", "Lithuania" => "LTU", "Luxembourg" => "LUX",
    "Malaysia" => "MYS", "Malta" => "MLT", "Marshall Islands" => "MHL", "Mauritius" => "MUS",
    "Mexico" => "MEX", "Monaco" => "MCO", "Namibia" => "NAM", "Netherlands" => "NLD",
    "Netherlands Antilles" => "ANT", "New Zealand" => "NZL", "Norway" => "NOR", "Oman" => "OMN",
    "Pakistan" => "PAK", "Panama" => "PAN", "Peru" => "PER", "Philippines" => "PHL",
    "Poland" => "POL", "Portugal" => "PRT", "Puerto Rico" => "PRI", "Qatar" => "QAT",
    "Russian Federation" => "RUS", "Samoa" => "WSM", "San Marino" => "SMR", "Saudi Arabia" => "SAU",
    "Singapore" => "SGP", "Slovenia" => "SVN", "South Africa" => "ZAF", "South Korea" => "KOR",
    "Spain" => "ESP", "St Vincent-Grenadines" => "VCT", "Swaziland" => "SWZ", "Sweden" => "SWE",
    "Switzerland" => "CHE", "Taiwan" => "TWN", "Thailand" => "THA", "Tunisia" => "TUN",
    "Turkey" => "TUR", "US Virgin Islands" => "VIR", "Uganda" => "UGA", "Ukraine" => "UKR",
    "United Arab Emirates" => "ARE", "United Kingdom" => "GBR", "United States" => "USA",
    "Uruguay" => "URY", "Vanuatu" => "VUT", "Viet Nam" => "VNM"
)

function main()
    time_start = time()
    println("Reading data...")
    fundsec_info = load_data_in_parts(INPUT_DIR)
    dropmissing!(fundsec_info, :fundid)

    println("Aggregating to fund level...")
    fund_info = groupby(fundsec_info, :fundid) |> gb->combine(
        gb, UNIQUE_AGG_COLS .=> agg_if_unique, "inception_date" => minimum; renamecols=false
    )

    fund_info.domicile = map(country -> ISO_MAP[country], fund_info.domicile)
    rename!(fund_info, RENAME_MAP)

    output_filestring = makepath(OUTPUT_DIR, "mf_info.arrow")
    Arrow.write(output_filestring, fund_info)

    duration_s = round(time() - time_start, digits=2)
    println("Finished aggregating fund info in $duration_s seconds.")
end

agg_if_unique(v) = length(unique(v)) == 1 ? first(v) : missing

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

dftest = Arrow.Table(joinpath(DIRS.fund, "post-processing", option_foldername(;DEFAULT_OPTIONS...), "main/fund_data.arrow")) |> DataFrame