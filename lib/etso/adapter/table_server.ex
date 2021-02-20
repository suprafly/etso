defmodule Etso.Adapter.TableServer do
  @moduledoc """
  The Table Server is a simple GenServer tasked with starting and holding an ETS table, which is
  namespaced in the Table Registry by the Repo and the Schema. Once the Table Server starts, it
  will attempt to create the ETS table, and also register the ETS table with the Table Registry.
  """

  use GenServer
  alias Etso.Adapter.TableRegistry

  @spec start_link({Etso.repo(), Etso.schema(), atom()}) :: GenServer.on_start()

  @doc """
  Starts the Table Server for the given `repo` and `schema`, with registration under `name`.
  """
  def start_link({repo, schema, name}) do
    GenServer.start_link(__MODULE__, {repo, schema}, name: name)
  end

  @impl GenServer
  def init({repo, schema}) do
    with table_name <- Module.concat([repo, schema]),
         table_path <- table_path(repo),
         table_reference <- PersistentEts.new(table_name, table_path, [:set, :public, :named_table]),
         :ok <- TableRegistry.register_table(repo, schema, table_reference) do
      {:ok, table_reference}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp table_path(repo_module) do
    repo_module
    |> to_string()
    |> String.replace_leading("Elixir.", "")
    |> String.downcase()
    |> String.replace(".", "_")
    |> Kernel.<>(".tab")
  end
end
