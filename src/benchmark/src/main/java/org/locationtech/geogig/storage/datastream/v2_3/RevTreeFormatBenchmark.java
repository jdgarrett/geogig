/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.locationtech.geogig.storage.datastream.v2_3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.model.impl.RevObjectTestSupport;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.ImmutableList;

public class RevTreeFormatBenchmark {

    @State(Scope.Thread)
    public abstract static class AbstractTreeBenchmark {

        public RevTree tree = null;

        public ByteArrayOutputStream out = new ByteArrayOutputStream();

        public byte[] serializedTree = null;

        public byte[] encodedTree = null;

        @Setup(Level.Iteration)
        public void beforeIteration() throws IOException {
            tree = buildTree();
            encodedTree = RevTreeFormat.encode(tree);
            DataStreamSerializationFactoryV2.INSTANCE.write(tree, out);
            serializedTree = out.toByteArray();
            out.reset();
        }

        protected abstract RevTree buildTree();

        @TearDown(Level.Invocation)
        public void tearDown() {
            out.reset();
        }

        @Benchmark
        @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
        @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        @Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        public byte[] EncodeTree() {
            return RevTreeFormat.encode(tree);
        }

        @Benchmark
        @BenchmarkMode({ Mode.SingleShotTime, Mode.AverageTime })
        @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        @Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        public RevTree DecodeTree() {
            return RevTreeFormat.decode(RevTree.EMPTY_TREE_ID, encodedTree);
        }

        @Benchmark
        @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
        @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        @Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        public void SerializeTree() throws IOException {
            DataStreamSerializationFactoryV2.INSTANCE.write(tree, out);
        }

        @Benchmark
        @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
        @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        @Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        public RevObject DeserializeTree() throws IOException {
            return DataStreamSerializationFactoryV2.INSTANCE.read(RevTree.EMPTY_TREE_ID,
                    new ByteArrayInputStream(serializedTree));
        }

    }

    public static class Empty extends AbstractTreeBenchmark {
        @Override
        protected RevTree buildTree() {
            return RevTree.EMPTY;
        }
    }

    public static class FeatureLeafWithNonRepeatedMetadataIds extends AbstractTreeBenchmark {
        @Override
        protected RevTree buildTree() {
            List<Node> tNodes = TestSupport.nodes(TYPE.TREE, 1024, true, true, false);
            List<Node> fNodes = TestSupport.nodes(TYPE.FEATURE, 1024, true, true, false);
            return TestSupport.tree(2048, tNodes, fNodes, null);
        }
    }

    public static class FeatureLeafWithNonRepeatedMetadataIdsAndExtraData
            extends AbstractTreeBenchmark {
        @Override
        protected RevTree buildTree() {
            List<Node> tNodes = TestSupport.nodes(TYPE.TREE, 1024, true, true, true);
            List<Node> fNodes = TestSupport.nodes(TYPE.FEATURE, 1024, true, true, true);
            return TestSupport.tree(2048, tNodes, fNodes, null);
        }
    }

    public static class FeatureLeafWithRepeatedMetadataIds extends AbstractTreeBenchmark {
        @Override
        protected RevTree buildTree() {
            List<ObjectId> repeatingMdIds = ImmutableList.of(//
                    RevObjectTestSupport.hashString("mdid1"), //
                    RevObjectTestSupport.hashString("mdid2"), //
                    RevObjectTestSupport.hashString("mdid3"));
            List<Node> tNodes = TestSupport.nodes(TYPE.TREE, 1024, repeatingMdIds, true, false);
            List<Node> fNodes = TestSupport.nodes(TYPE.FEATURE, 1024, repeatingMdIds, true, false);
            return TestSupport.tree(2048, tNodes, fNodes, null);
        }
    }

    public static class FeatureLeafWithRepeatedMetadataIdsAndExtraData
            extends AbstractTreeBenchmark {
        @Override
        protected RevTree buildTree() {
            List<ObjectId> repeatingMdIds = ImmutableList.of(//
                    RevObjectTestSupport.hashString("mdid1"), //
                    RevObjectTestSupport.hashString("mdid2"), //
                    RevObjectTestSupport.hashString("mdid3"));
            List<Node> tNodes = TestSupport.nodes(TYPE.TREE, 1024, repeatingMdIds, true, true);
            List<Node> fNodes = TestSupport.nodes(TYPE.FEATURE, 1024, repeatingMdIds, true, true);
            return TestSupport.tree(2048, tNodes, fNodes, null);
        }
    }

    public static class Buckets extends AbstractTreeBenchmark {
        @Override
        protected RevTree buildTree() {
            SortedMap<Integer, Bucket> buckets = new TreeMap<>();
            buckets.put(1, Bucket.create(RevObjectTestSupport.hashString("b1"), null));
            return TestSupport.tree(1024, null, null, buckets);
        }
    }
}
