namespace storage;

public class SSTable {
  readonly string dataDirPath;
  readonly string indexDirPath;
  public SSTable(string dataDirPath, string indexDirPath) {
    this.dataDirPath = dataDirPath;
    this.indexDirPath = indexDirPath;

    if(!Directory.Exists(dataDirPath)) {
      Directory.CreateDirectory(dataDirPath);
    }
    if(!Directory.Exists(indexDirPath)) {
      Directory.CreateDirectory(indexDirPath);
    }
  }
  public async void WriteData<T>(T data) where T: IDictionary<string, string> {
    string filename = DateTime.Now.Ticks.ToString();
    string filepath = $"{dataDirPath}/{filename}.data";
    File.Create(filepath).Close();
    var index = new Dictionary<string, long>();
    StreamWriter w = new(filepath);

    long currentFilePosition = 0;
    foreach (var item in data) {
      index[item.Key] = currentFilePosition;
      await w.WriteLineAsync(item.Value);
      // The +1 represents the '\n' char since we are writing line
      currentFilePosition += item.Value.Length + 1;
    }
    w.Close();
    SaveIndex(filename, index);
  }

  public void SaveIndex(string filename, Dictionary<string, long> index) {
    string filepath = $"{indexDirPath}/{filename}.index";
    File.Create(filepath).Close();
    StreamWriter w = new(filepath);
    foreach(var item in index) {
      w.WriteLine($"{item.Key}.{item.Value}");
    }
    w.Close();
  }

  public static Dictionary<string, long> ParseIndex(string indexpath) {
    var r = new StreamReader(indexpath);
    var result = new Dictionary<string, long>();
    while(true) {
      string? line = r.ReadLine();
      if(line == null) {
        r.Close();
        return result;
      }
      string[] values = line.Split('.');
      string key = values[0];
      long offset = long.Parse(values[1]);

      result[key] = offset;
    }
  }
  public string? Get(string key) {
    var files = Directory.EnumerateFiles(dataDirPath).ToList();
    foreach(string filepath in files) {
      string filename = Path.GetFileNameWithoutExtension(filepath);
      
      string correspondingIndexPath = $"{this.indexDirPath}/{filename}.index";
      var index = ParseIndex(correspondingIndexPath);
      if(index.ContainsKey(key)) {
        long offset = index[key];
        FileStream f = new(filepath, FileMode.Open);
        f.Seek(offset, SeekOrigin.Begin);
        StreamReader r = new(f);
        Console.WriteLine($"Offset: {offset}");
        string? value = r.ReadLine();
        if(value != null) {
          return value;
        }
      }

    }
    return null;
  }
}