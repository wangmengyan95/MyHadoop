package myhadoop.fs;

import java.io.Serializable;
import java.util.List;

public class Path implements Serializable{
	private String path;

	public Path(String path) {
		this.path = path;
	}

	public Path(String path, String subPath) {
		if (path.endsWith("/")) {
			this.path = path + subPath;
		}
		else {
			this.path = path + "/" + subPath;
		}
	}

	public Path(Path path, String subPath) {
		if (path.getPath().endsWith("/")) {
			this.path = path.getPath() + subPath;
		}
		else {
			this.path = path.getPath() + "/" + subPath;
		}
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
