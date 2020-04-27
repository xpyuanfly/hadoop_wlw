package cn.hut.hdfs_demo;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DemoApplicationTests {

	private HdfsDemo hd = new HdfsDemo();

	@Test
	void contextLoads() {
	}

	@Before
	void beforeCall(){
		hd.init();
	}

	@Test
	void get(){
		hd.copyToLocalFile();
	}

	@Test
	void get2Stream(){
		hd.get2Stream();
	}
}
