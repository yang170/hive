/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.objectinspector;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import com.google.common.collect.Lists;


import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * TestReflectionObjectInspectors.
 *
 */
public class TestReflectionObjectInspectors {

  @Test
  public void testReflectionObjectInspectors() throws Throwable {
    try {
      ObjectInspector oi1 = ObjectInspectorFactory
          .getReflectionObjectInspector(MyStruct.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      ObjectInspector oi2 = ObjectInspectorFactory
          .getReflectionObjectInspector(MyStruct.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      assertEquals(oi1, oi2);

      // metadata
      assertEquals(Category.STRUCT, oi1.getCategory());
      StructObjectInspector soi = (StructObjectInspector) oi1;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      assertEquals(6, fields.size());
      assertNotEquals(-1, fields.indexOf(soi.getStructFieldRef("myString")));

      // null
      for (int i = 0; i < fields.size(); i++) {
        assertNull(soi.getStructFieldData(null, fields.get(i)));
      }
      assertNull(soi.getStructFieldsDataAsList(null));

      // non nulls
      MyStruct a = new MyStruct();
      a.myInt = 1;
      a.myInteger = 2;
      a.myString = "test";
      a.myStruct = a;
      a.myListString = Arrays.asList(new String[] {"a", "b", "c"});
      a.myMapStringString = new HashMap<String, String>();
      a.myMapStringString.put("key", "value");

      int myIntIdx = IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx)
        .getFieldName()
        .equals("myint"))
        .findFirst()
        .orElse(-1);
      int myIntegerIdx = IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx)
        .getFieldName()
        .equals("myinteger"))
        .findFirst()
        .orElse(-1);
      int myStringIdx = IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx)
        .getFieldName()
        .equals("mystring"))
        .findFirst()
        .orElse(-1);
      int myStructIdx = IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx)
        .getFieldName()
        .equals("mystruct"))
        .findFirst()
        .orElse(-1);
      int myListIdx = IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx)
        .getFieldName()
        .equals("myliststring"))
        .findFirst()
        .orElse(-1);
      int myMapStringStringIdx = IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx)
        .getFieldName()
        .equals("mymapstringstring"))
        .findFirst()
        .orElse(-1);
      
      assertNotEquals(-1, myIntIdx);
      assertNotEquals(-1, myIntegerIdx);
      assertNotEquals(-1, myStringIdx);
      assertNotEquals(-1, myStructIdx);
      assertNotEquals(-1, myListIdx);
      assertNotEquals(-1, myMapStringStringIdx);
      
      assertEquals(1, soi.getStructFieldData(a, fields.get(myIntIdx)));
      assertEquals(2, soi.getStructFieldData(a, fields.get(myIntegerIdx)));
      assertEquals("test", soi.getStructFieldData(a, fields.get(myStringIdx)));
      assertEquals(a, soi.getStructFieldData(a, fields.get(myStructIdx)));
      assertEquals(a.myListString, soi.getStructFieldData(a, fields.get(myListIdx)));
      assertEquals(a.myMapStringString, soi
          .getStructFieldData(a, fields.get(myMapStringStringIdx)));
      ArrayList<Object> afields = new ArrayList<Object>();
      for (int i = 0; i < 6; i++) {
        afields.add(soi.getStructFieldData(a, fields.get(i)));
      }
      assertEquals(afields, soi.getStructFieldsDataAsList(a));

      // sub fields
      assertEquals(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          fields.get(myIntIdx).getFieldObjectInspector());
      assertEquals(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          fields.get(myIntegerIdx).getFieldObjectInspector());
      assertEquals(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          fields.get(myStringIdx).getFieldObjectInspector());
      assertEquals(soi, fields.get(myStructIdx).getFieldObjectInspector());
      assertEquals(
          ObjectInspectorFactory
              .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
          fields.get(myListIdx).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector), fields
          .get(myMapStringStringIdx).getFieldObjectInspector());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testObjectInspectorMaxCacheSize() {
    int maxSize = 10240;
    for (int i = 0; i < maxSize; i++) {
      ObjectInspectorFactory
        .getStandardUnionObjectInspector(Lists.newArrayList(new JavaConstantStringObjectInspector("" + i)));
    }
    assertTrue("Got: " + ObjectInspectorFactory.cachedStandardUnionObjectInspector.size(),
      ObjectInspectorFactory.cachedStandardUnionObjectInspector.size() <= maxSize);
    for (int i = 0; i < 1000; i++) {
      ObjectInspectorFactory.getStandardUnionObjectInspector(Lists.newArrayList(new
        JavaConstantStringObjectInspector("" + (10240 + i))));
    }
    assertTrue("Got: " + ObjectInspectorFactory.cachedStandardUnionObjectInspector.size(), ObjectInspectorFactory
      .cachedStandardUnionObjectInspector.size() <= maxSize);
  }

  @Test
  public void testObjectInspectorThreadSafety() throws InterruptedException {
    final int workerCount = 5; // 5 workers to run getReflectionObjectInspector concurrently
    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(workerCount);
    final MutableObject exception = new MutableObject();
    Thread runner = new Thread(new Runnable() {
      @Override
      @SuppressWarnings("unchecked")
      public void run() {
        Future<ObjectInspector>[] results = (Future<ObjectInspector>[])new Future[workerCount];
        Pair<Type, ObjectInspectorFactory.ObjectInspectorOptions>[] types =
          (Pair<Type, ObjectInspectorFactory.ObjectInspectorOptions>[])new Pair[] {
             Pair.of(Complex.class, ObjectInspectorFactory.ObjectInspectorOptions.THRIFT),
             Pair.of(MyStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
          };
        try {
          for (int i = 0; i < 20; i++) { // repeat 20 times
            for (final Pair<Type, ObjectInspectorFactory.ObjectInspectorOptions> t: types) {
              ObjectInspectorFactory.objectInspectorCache.asMap().clear();
              for (int k = 0; k < workerCount; k++) {
                results[k] = executorService.schedule(new Callable<ObjectInspector>() {
                  @Override
                  public ObjectInspector call() throws Exception {
                    return ObjectInspectorFactory.getReflectionObjectInspector(
                      t.getLeft(), t.getRight());
                  }
                }, 50, TimeUnit.MILLISECONDS);
              }
              ObjectInspector oi = results[0].get();
              for (int k = 1; k < workerCount; k++) {
                assertEquals(oi, results[k].get());
              }
            }
          }
        } catch (Throwable e) {
          exception.setValue(e);
        }
      }
    });
    try {
      runner.start();
      long endTime = System.currentTimeMillis() + 300000; // timeout in 5 minutes
      while (runner.isAlive()) {
        if (System.currentTimeMillis() > endTime) {
          runner.interrupt(); // Interrupt the runner thread
          fail("Timed out waiting for the runner to finish");
        }
        runner.join(10000);
      }
      if (exception.getValue() != null) {
        fail("Got exception: " + exception.getValue());
      }
    } finally {
      executorService.shutdownNow();
    }
  }
}
