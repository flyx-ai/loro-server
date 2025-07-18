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
doc.doc.commit();
console.log(doc.doc.toJSON());
setTimeout(() => {
  console.log(doc.doc.toJSON());
}, 2000);
