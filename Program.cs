namespace main;

using storage;

internal class Program {
  public static void Main(string[] args) {
    Lsm lsm = new("examples/.data", "examples/.index", 5);

    AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => {
      lsm.Flush();
    };

    string? found = lsm.Get("key3");

    Console.WriteLine(found);
  }
}