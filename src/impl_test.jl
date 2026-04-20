module Solution
export __init2__

using Base.Threads
using Mmap

const amountOfLines = 100_000_000
testLock = ReentrantLock()

mutable struct Measurement
  @atomic min::Float32
  @atomic max::Float32
  @atomic sum::Float32
  @atomic amountOfPoints::Int64
end

@inline function fastparse(data::AbstractVector{UInt8}, i::Int)
    neg = data[i] == UInt8('-')
    i += neg

    d1 = data[i] - 0x30
    i += 1

    if data[i] == UInt8('.')   # one digit before decimal
        frac = data[i+1] - 0x30
        val = d1 + 0.1f0 * frac
        return neg ? -val : val
    else                       # two digits before decimal
        d2 = data[i] - 0x30
        i += 1  # now at '.'
        frac = data[i+1] - 0x30
        val = (10*d1 + d2) + 0.1f0 * frac
        return neg ? -val : val
    end
end

function efficientSplit(line::String, delim::Char)
  i = findfirst(==(delim), line)
  @inbounds begin
    name = SubString(line, firstindex(line), prevind(line, i))
    # TODO: Parse is heavy, as it has to make a expensive c call. Try and fix
    val = parse(Float32, SubString(line, nextind(line, i), lastindex(line)))
    return name, val
  end
end

function sortData(dataPoints)
  # Copy the keys ourselves into a vector, to prevent having to grow the vector
  # If we would have just called collect, it would have had to grow the vector
  ks = Vector{String}(undef, length(dataPoints))
  i = 1
  for k in keys(dataPoints)
    ks[i] = k
    i += 1
  end
  return sort!(ks, alg=QuickSort)
end

function printData(dataPoints)
  print("{")

  sortedKeys = sortData(dataPoints)

  io = IOBuffer()

  for key in sortedKeys
    value = dataPoints[key]
    print(key)
    print("=")
    print(round(value.min, digits=1))
    print("/")
    print(round(value.sum/value.amountOfPoints, digits=1))
    print("/")
    print(round(value.max, digits=1))
    print(", ")
  end

  print("}")
end

function processData2(data, ranges)
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  @inbounds for (i, semiColonIndex) in ranges
    station = unsafe_string(pointer(data) + (i - 1), semiColonIndex - i)
    value = fastparse(data, semiColonIndex + 1)

    if haskey(dataPoints, station)
      existingPoint = dataPoints[station]
      @atomic min(existingPoint.min, value)
      @atomic max(existingPoint.max, value)
      @atomic existingPoint.sum += value
      @atomic existingPoint.amountOfPoints += 1
    else
      push!(dataPoints, station => Measurement(value, value, value, 1))
    end
  end

  return dataPoints
end

function processData(data)
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  @inbounds for point in data
    station, value = efficientSplit(point, ';') 

    if haskey(dataPoints, station)
      existingPoint = dataPoints[station]
      @atomic min(existingPoint.min, value)
      @atomic max(existingPoint.max, value)
      @atomic existingPoint.sum += value
      @atomic existingPoint.amountOfPoints += 1
    else
      push!(dataPoints, station => Measurement(value, value, value, 1))
    end
  end

  return dataPoints
end

function mergeResults(fullResult::Dict{String, Measurement}, partialResults::Dict{String, Measurement})
  global testLock

  for partialMeasurement in partialResults
    lock(testLock)
    existingPoint = get!(fullResult, partialMeasurement.first, partialMeasurement.second)
    unlock(testLock)

    if existingPoint != partialMeasurement
      # existingPoint = fullResult[partialMeasurement.first]

      @atomic min(existingPoint.min, partialMeasurement.second.min)
      @atomic max(existingPoint.max, partialMeasurement.second.max)
      @atomic existingPoint.sum += partialMeasurement.second.sum
      @atomic existingPoint.amountOfPoints += partialMeasurement.second.amountOfPoints
    end
  end
end

function __init2__()
  GC.enable(false)
  path = "./data/measurements_100m.txt"
  data = Mmap.mmap(open(path, "r"))
  i = 1
  len = filesize(path)

  semiColonChar = UInt8(';')
  newLineChar = UInt8('\n')

  lines = Vector{Tuple{Int, Int}}()
  sizehint!(lines, 100_000_000)

  # chunkSize = 10_000_000
  chunkSize = fld(100_000_000, 15)
  items = 0

  mergedResults = Dict{String, Measurement}()

  @sync while i < len
    semiColonIndex = i
    while data[semiColonIndex] != semiColonChar
      semiColonIndex += 1
    end

    valueIndex = semiColonIndex + 1 # TODO: Might be able to make this slightly faster, as we know the smallest possible value must be at least 3 chars (ex. 1.2), so we might aswell skip 3 values to prevent the lookups
    while valueIndex <= len && data[valueIndex] != newLineChar
      valueIndex += 1
    end

    push!(lines, (i, semiColonIndex))
    items += 1

    if items % chunkSize == 0
      @spawn begin
        ranges = @view lines[items - chunkSize + 1:items]
        dataPoints = processData2(data, ranges)
        mergeResults(mergedResults, dataPoints)
      end
    end
    
    i = valueIndex + 1
  end
end

function test()
  open("./data/measurements_100m.txt", "r") do io
    for line in eachline(io)
    end
  end
end

# function __init__()
#   GC.enable(false) # Disable GC, as we run for a short period of time anyways, and there is no point in dealing with the overhead
#
#   # *4 seems to give the most optimal result when ran with 15 threads. 
#   # It might have to do with that reading speed of the file is the limiting factor, so if you make the chunkSize too large, most threads spend their time doing waiting for data
#   amountOfThreads = nthreads() * 40
#   # chunkSize = fld(amountOfLines, amountOfThreads)
#   chunkSize = 100_000 
#
#   mergedResults = Dict{String, Measurement}()
#   sizehint!(mergedResults, 10_000) # The challange said that there could at max be 10_000 stations
#
#   mergerChannel = Channel{Dict{String, Measurement}}(amountOfThreads)
#   processorChannel = Channel{SubArray}(amountOfThreads)
#
#   merger = @spawn begin
#     for dataPoints in mergerChannel
#       mergeResults(mergedResults, dataPoints)
#     end
#   end
#
#   for _ in 1:nthreads() -1
#     @spawn begin
#       for item in processorChannel
#         dataPoints = processData(dataView)
#         put!(mergerChannel, dataPoints)
#       end
#     end
#   end
#
#   open("./data/measurements_100m.txt", "r") do io
#     i = 1
#     lines = Vector{String}(undef, amountOfLines)
#     for line in eachline(io)
#       lines[i] = line
#       i += 1
#
#       if i % chunkSize == 0
#         t = fld(i, chunkSize)
#         lo = (t - 1) * chunkSize + 1
#         hi = t == amountOfThreads ? amountOfLines : t * chunkSize
#
#         dataView = @view lines[lo:hi]
#         put!(processorChannel, dataView)
#         # @spawn begin 
#         #  dataPoints = processData(dataView)
#         #  put!(mergerChannel, dataPoints)
#         #  # mergeResults(mergedResults, dataPoints)
#         # end
#       end
#     end
#   end
#   close(mergerChannel)
#   wait(merger)
#
#   close(processorChannel)
#
#   # printData(mergedResults)
# end

# @time __init2__()
# @time test()
# sleep(1)
end
