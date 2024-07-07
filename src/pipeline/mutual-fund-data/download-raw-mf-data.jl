using Revise
using DataFrames
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function download_raw_mf_data()
    task_start = time()

    tables = [
        "fund_hdr",
        "fund_hdr_hist",
        "monthly_nav",
        "monthly_tna",
        "monthly_returns",
        "dividends",
        "contact_info",
        "front_load",
        "rear_load",
        "fund_fees",
        "fund_style"
    ]

    wrds = connect_wrds()
    println("Connection to WRDS established.")
    println()
    for table in tables
        subtask_start = time()
        println("Downloading table \"$table\"...")

        data = query_wrds(wrds, "select * from crsp.$table")
        output_filename = joinpath(DIRS.mf.raw, "$table.arrow")
        Arrow.write(output_filename, data)
        printtime(
            "downloading \"$table\"", task_start;
            process_start_time=subtask_start, minutes=true
        )
        println()
    end
    close(wrds)
    println("Connection closed.")

    printtime("downloading raw mutual fund data", task_start, minutes=true)
end

if abspath(PROGRAM_FILE) == @__FILE__
    download_raw_mf_data()
end