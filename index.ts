import { CRDTDoc } from "./lorojs/loro-client";

await fetch("http://localhost:8080/api/v1/document/test", {
  method: "POST",
});

const doc = new CRDTDoc(
  "test",
  "http://localhost:8080/api/v1/document/test/ws",
);

const a = doc.doc.getList("a");
a.insert(0, "A");
a.insert(1, "hi");
const b = doc.doc.getMap("b");
b.set("key1", "value1");
b.set("key2", "value2");
doc.doc.commit();
setTimeout(() => {
  console.log(doc.doc.toJSON());
}, 1000);
