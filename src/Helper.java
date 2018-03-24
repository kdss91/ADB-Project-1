
public class Helper {
	static public long getAvailableMemory() {
		System.gc();
		return Runtime.getRuntime().freeMemory();
	}

	static public long getMaxMemory() {
		System.gc();
		return Runtime.getRuntime().totalMemory();
	}
}
