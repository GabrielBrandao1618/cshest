namespace main;

using storage;

internal class Program {
  public static void Main(string[] args) {
    Lsm lsm = new("examples/.data", "examples/.index", 4);
    lsm.Set("key1", "Apple");
    lsm.Set("key2", "Grape");
    lsm.Set("key3", "Orange");
    lsm.Set("key4", "Banana");
    lsm.Set("key5", "Peach");

    string? found = lsm.Get("key1");

    Console.WriteLine(found);
  }
}