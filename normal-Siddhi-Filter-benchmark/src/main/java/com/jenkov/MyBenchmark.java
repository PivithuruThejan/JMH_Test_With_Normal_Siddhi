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

package com.jenkov;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Tests performance of "Siddhi".
 */
public class MyBenchmark {

    /**
     * Inserts 11 million events to "Siddhi".
     *
     * @param state
     * @throws InterruptedException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IOException
     * @throws IllegalAccessException
     */
    @Benchmark
    @BenchmarkMode(Mode.All)
    public void testMethod(StartUpState state, Blackhole blackhole) throws InterruptedException, InvocationTargetException,
            InstantiationException, IOException, IllegalAccessException {
        state.CHECK = 0;
        for (int i = 0; i < state.COUNT; i++) {
            state.inputHandler.send(state.o1);
            state.inputHandler.send(state.o2);
            state.inputHandler.send(state.o3);
            state.inputHandler.send(state.o4);
            state.inputHandler.send(state.o5);
            state.inputHandler.send(state.o6);
            state.inputHandler.send(state.o7);
            state.inputHandler.send(state.o8);
            state.inputHandler.send(state.o9);
            state.inputHandler.send(state.o10);
            state.inputHandler.send(state.o11);
        }

        blackhole.consume(state.CHECK);
    }

    /**
     * Handles operations that need not to be tested.
     */
    @State(Scope.Benchmark)
    public static class StartUpState {
        private static final int COUNT = 1000000;
        public static int CHECK = 0;
        public String definition = "@config(async = 'true') define stream players(playerName string,country string," +
                "TestAverage float,TestStrikeRate float,ODIAverage float,ODIStrikeRate float,T20Average float," +
                "T20StrikeRate float,BattingStyle string);";
        public String query = "@info(name = 'query1') from players[(TestAverage>45.0 and TestStrikeRate>45.0 or ODIAverage>45.0)" +
                " and (ODIAverage>40.0 or ODIStrikeRate>100.0) and not(T20Average<10.0 or T20StrikeRate>150.0 and TestStrikeRate>65.0)" +
                " or (ODIAverage<35.0 or T20StrikeRate>130.0 and not(TestStrikeRate < 55.0))] select playerName, BattingStyle insert into sqaud;";
        public SiddhiManager siddhiManager = new SiddhiManager();
        public SiddhiAppRuntime siddhiAppRuntime;
        public InputHandler inputHandler;
        public Object[] o1 = new Object[]{"Upul Tharanga", "Sri Lanka", 33.0f, 62.5f, 32.5f, 80.5f, 16.3f, 116.3f, "LHB"};
        public Object[] o2 = new Object[]{"Anjelo Mathews", "Sri Lanka", 47.6f, 65.3f, 42.1f, 83.4f, 26.3f, 136.3f, "RHB"};
        public Object[] o3 = new Object[]{"Asela Gunaratne", "Sri Lanka", 53.7f, 57.4f, 36.5f, 85.5f, 36.3f, 146.7f, "RHB"};
        public Object[] o4 = new Object[]{"Joe Root", "England", 55.8f, 52.5f, 52.7f, 88.3f, 24.9f, 128.3f, "RHB"};
        public Object[] o5 = new Object[]{"Ben Stokes", "England", 41.2f, 72.5f, 43.6f, 90.7f, 22.3f, 133.8f, "LHB"};
        public Object[] o6 = new Object[]{"Kane Williamson", "New Zealand", 54.2f, 48.7f, 45.1f, 79.3f, 29.3f, 119.3f, "RHB"};
        public Object[] o7 = new Object[]{"Steve Smith", "Australia", 63.3f, 51.5f, 50.5f, 82.7f, 16.3f, 112.2f, "RHB"};
        public Object[] o8 = new Object[]{"AB de Villiers", "South Africa", 51.9f, 62.1f, 52.5f, 101.5f, 33.3f, 156.3f, "RHB"};
        public Object[] o9 = new Object[]{"Hashim Amla", "South Africa", 47.8f, 47.5f, 52.5f, 86.5f, 26.3f, 127.3f, "RHB"};
        public Object[] o10 = new Object[]{"Virat Kholi", "India", 52.0f, 66.5f, 53.5f, 89.5f, 30.3f, 136.3f, "RHB"};
        public Object[] o11 = new Object[]{"Rohit Sharma", "India", 32.0f, 62.5f, 42.5f, 93.5f, 26.3f, 141.3f, "RHB"};

        public StartUpState() {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(definition + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    CHECK++;
                }

            });
            inputHandler = siddhiAppRuntime.getInputHandler("players");
        }
    }

}
