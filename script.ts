import { CRDTAwareness } from "./lorojs/loro-client.ts";

// createDocument("http://localhost:8080", "babysheet");

// async function test() {
//   let id = Math.floor(Math.random() * 1000000).toString();
//   await createDocument("http://localhost:8080", id);
//   const doc = new CRDTDoc(id, "http://localhost:8080");
//   const a = doc.doc.getList("a");
//   for (let i = 0; i < 100000; i++) {
//     a.push("A");
//   }
//   const b = doc.doc.getMap("b");
//   for (let i = 0; i < 100000; i++) {
//     b.set(i.toString(), i.toString());
//   }
//   doc.doc.commit();
//   // const timeout = Promise.withResolvers<void>();
//   // setTimeout(() => {
//   //   timeout.resolve();
//   // }, 100);
//   // await timeout.promise;
//   doc.destroy();
//   await doc.purge();
// }
//
// let count = 100;
// let parallel = 10;
//
// async function worker() {
//   while (true) {
//     const new_count = --count;
//     if (new_count < 0) {
//       break;
//     }
//     await test();
//   }
// }
//
// const workers: Promise<void>[] = [];
// for (let i = 0; i < parallel; i++) {
//   workers.push(worker());
// }
// Promise.all(workers)
//   .then(() => {
//     console.log("All tests completed.");
//   })
//   .catch((err) => {
//     console.error("Error during tests:", err);
//   });

async function hello() {
  const awareness1 = new CRDTAwareness("awareness", "http://localhost:8080");
  awareness1.store.set("username", "hello");
  console.log(awareness1.store.get("username"));
  await new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, 100);
  });
  const awareness2 = new CRDTAwareness("awareness", "http://localhost:8080");
  await new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, 100);
  });
  console.log(awareness2.store.get("username"));
  awareness1.destroy();
  awareness2.destroy();
}

hello();
