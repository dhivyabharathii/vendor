  defmodule Jobs do
    @moduledoc """
    Module for processing job updates.
    """

    @doc """
    Reads content in the file

    Returns `:ok`

    ## Examples
    iex(1)> file_path="final.txt"
    "final.txt"
    iex(2)> Jobs.read(file_path)
    {:ok, "contents in the file"}
    """
    def read(file_path) do
      File.read(file_path)
    end

    @doc """
    Filters updates based on farm ID.
    """
    def filter_updates(updates) do
      Enum.filter(updates, fn update ->
        update["farm_id"] == 3
      end)
    end
    @doc """
    Extracting the necessary details drom the given data.
    When tab is found , moving the contents to the next line
    """
    def format_file(input_file, output_file) do
      input_stream = File.stream!(input_file)
      output_stream = File.open!(output_file, [:write])

      Enum.each(input_stream, fn line ->
        parts = String.split(line, "\t")
        output = Enum.join(parts, "\n")
        output =
          if String.ends_with?(line, "\n") do
            output
          else
            output <> "\n"
          end

        IO.write(output_stream, output)
      end)

      File.close(output_stream)
    end

    @doc """
    Write the necessary line in a file
    """
    def write_lines(input_file, output_file) do
      input_stream = File.stream!(input_file)
      output_stream = File.open!(output_file, [:write])

      Enum.with_index(input_stream)
      |> Enum.each(fn {line, index} ->
        if rem(index + 1, 3) == 0 do
          line_with_comma = line <> ","
          IO.write(output_stream, line_with_comma)
        end
      end)

      File.close(output_stream)
    end

    @doc """
    Calculates the time difference between update and job timestamps.
    """
    def calculate_time_difference(updates) do
      final_map = Enum.reduce(updates, %{}, fn update, acc ->
        timestamp = parse_iso8601(update["timestamp"])
        Enum.reduce(update["jobs"], acc, fn job, acc ->
          job_id = job["Id"]
          job_timestamp = parse_iso8601(job["UpdatedAt"])
          time_difference_in_seconds = abs(NaiveDateTime.diff(job_timestamp, timestamp))

          case Map.get(acc, job_id) do
            nil -> Map.put(acc, job_id, time_difference_in_seconds)
            existing_time_difference when existing_time_difference > time_difference_in_seconds ->
              Map.put(acc, job_id, time_difference_in_seconds)
            _ -> acc
          end
        end)
      end)

      write_to_file(final_map, "Latency.txt")
      final_map
    end

    defp write_to_file(map, file_path) do
      {:ok, file} = File.open(file_path, [:write])

      Enum.each(map, fn {job_id, time_diff} ->
        formatted_line = "#{job_id} => #{time_diff}\n"
        IO.write(file, formatted_line)
      end)

      File.close(file)
    end

    defp parse_iso8601(timestamp) do
      case NaiveDateTime.from_iso8601(timestamp) do
        {:ok, dt} -> dt
        :error -> nil
      end
    end
  end



  # Jobs.format_file("v1_jobs_1h.txt","output.txt")
  # Jobs.write_lines("output.txt","final1.json")
  file_path = "final1.json"
  {:ok, file_content} = Jobs.read(file_path)
  updates = Jason.decode!(file_content)
  filtered_updates = Jobs.filter_updates(updates)
  time_difference=Jobs.calculate_time_difference(filtered_updates)
  IO.inspect(time_difference)
  IO.inspect(filtered_updates)
