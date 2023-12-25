# spark-practice
本项目基于 JDK 17，Spark 3.4.1。<br>

## 注意事项
自 JDK 17 开始，禁止通过反射方式访问不被导出的内部类，解决办法是添加如下虚拟机配置：`--add-exports java.base/sun.nio.ch=ALL-UNNAMED`<br>
