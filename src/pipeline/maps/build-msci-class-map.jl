using Revise
using DataFrames
using CSV
using Dates

include("../../shared/CommonConstants.jl")
include("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function build_msci_class_map()
    task_start = time()
    current_filepath = joinpath(DIRS.map.raw, "msci-class-current.csv")
    changes_filepath = joinpath(DIRS.map.raw, "msci-class-changes.csv")

    read_dateformat = DateFormat("d/mm/yyyy")
    msci_class_map = CSV.read(current_filepath, DataFrame, dateformat=read_dateformat)
    class_changes = CSV.read(changes_filepath, DataFrame, dateformat=read_dateformat)

    _populate_historical_classes!(msci_class_map, class_changes)
    sort!(msci_class_map, [:country_code, :date])
    
    printtime("building MSCI class map", task_start, minutes=false)
    return msci_class_map
end

function _populate_historical_classes!(msci_class_map, class_changes)
    assert_error = "Initial class map has more than one date: $msci_class_map"
    @assert length(unique(msci_class_map.date)) == 1 assert_error
    penultimate_date = first(msci_class_map.date) - Month(1)
    earliest_date = Date(1990, 1, 1)

    for date in penultimate_date:Month(-1):earliest_date
        _generate_prior_classes!(msci_class_map, class_changes, date)
    end
end

function _generate_prior_classes!(msci_class_map, class_changes, date)
    lookahead_date = date + Month(1)

    # A change on date D marks a difference between the classes on date D-1 and date D,
    # so the changes to apply on date D are those marked for date D+1.
    changes_on_date = class_changes[class_changes.date .== lookahead_date, :]
    classes_to_copy = _carry_down_classes(msci_class_map, lookahead_date)
    
    nrow(changes_on_date) > 0 && _apply_changes!(classes_to_copy, changes_on_date)
    append!(msci_class_map, classes_to_copy)

    return
end

function _carry_down_classes(msci_class_map, lookahead_date)
    classes_to_copy = msci_class_map[msci_class_map.date .== lookahead_date, :]

    classes_to_copy.date .-= Month(1)

    return classes_to_copy
end

function _apply_changes!(classes_to_copy, changes_on_date)
    for change_instruction in eachrow(changes_on_date)
        target_country = change_instruction.country_code
        target_class = change_instruction.change_from
        target_date = change_instruction.date - Month(1)
        class_before_change = change_instruction.change_to
        target_row(x) = (x.country_code .== target_country)

        _assert_valid_change(
            classes_to_copy[target_row(classes_to_copy), :],
            class_before_change
        )

        if ismissing(change_instruction.change_to)
            append!(
                classes_to_copy,
                DataFrame(
                    country_code = target_country,
                    date = target_date,
                    msci_class = target_class,
                )
            )
        elseif ismissing(target_class)
            filter!((!target_row), classes_to_copy)
        else
            classes_to_copy[target_row(classes_to_copy), :msci_class] .= target_class
        end
    end

    return
end

function _assert_valid_change(copied_row, class_before_change)
    assert_error = "Invalid change instruction\n\n$copied_row\n$class_before_change\n"
    if ismissing(class_before_change)
        @assert nrow(copied_row) == 0 assert_error * "(missing class)"
    else
        @assert nrow(copied_row) == 1 assert_error * "(non-missing class)"
        @assert first(copied_row.msci_class) == class_before_change assert_error
    end

    return
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = build_msci_class_map()
    output_filename = makepath(DIRS.map.refined, "msci-class.csv")

    task_start = time()
    CSV.write(output_filename, output_data)
    printtime("writing MSCI class map", task_start, minutes=false)
end