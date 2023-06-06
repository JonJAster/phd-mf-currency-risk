function printtime(
    task::AbstractString, start_time::Number;
    process_subtask::AbstractString="", process_start_time::Number=0, minutes=false)
    if isempty(process_subtask) ⊻ iszero(process_start_time)
        error("If any process parameters are supplied, all must be supplied")
    end
    timed_process = !isempty(process_subtask)

    duration_s = round(time() - start_time, digits=2)
    duration_m = round(duration_s / 60, digits=2)
    
    if !timed_process
        printout = "Finished $task in $duration_s seconds"
        minutes && (printout *= " ($duration_m minutes)")
    else
        process_duration_s = round(time() - process_start_time, digits=2)
        process_duration_m = round(process_duration_s / 60, digits=2)

        printout = "Finished $task for $process_subtask in $process_duration_s seconds"
        minutes && (printout *= " ($process_duration_m minutes)")
        printout *= ", total running time $duration_s seconds"
        minutes && (printout *= " ($duration_m minutes)")
    end

    println(printout)
    return nothing
end