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
  public void WriteData(Dictionary<string, string> data) {
    string filename = DateTime.Now.Ticks.ToString();
    string filepath = $"{dataDirPath}/{filename}.data";
    File.Create(filepath).Close();
    var index = new Dictionary<string, long>();
    StreamWriter w = new(filepath);

    long currentFilePosition = 0;
    foreach (var item in data) {
      index[item.Key] = currentFilePosition;
      w.WriteLine(item.Value);
      currentFilePosition += item.Value.Length;
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
        StreamReader r = new(filepath);
        r.BaseStream.Position = offset;
        string? value = r.ReadLine();
        if(value != null) {
          return value;
        }
      }

    }
    return null;
  }
}