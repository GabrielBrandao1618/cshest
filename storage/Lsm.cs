namespace storage;

public class Lsm {
  Dictionary<string, string> memTable = [];
  readonly SSTable ssTable;
  readonly int flushThreshold;

  public Lsm(string dataDirPath, string indexDirPath, int flushThreshold) {
    ssTable = new(dataDirPath, indexDirPath);
    this.flushThreshold = flushThreshold;
  }
  ~Lsm() {
    Flush();
  }

  public string? Get(string key) {
    memTable.TryGetValue(key, out string? fromMemory);
    if(fromMemory != null) {
      return fromMemory;
    }
    var fromDisk = ssTable.Get(key);
    return fromDisk;
  }
  public void Set(string key, string val) {
    memTable[key] = val;
    if(memTable.Count > flushThreshold) {
      Flush();
    }
  }
  public void Flush() {
    ssTable.WriteData(memTable);
    memTable = [];
  }
}