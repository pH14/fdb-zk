package com.ph14.fdb.zk.curator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

public class LeaderElection {

  @Test
  public void itRunsALeaderElection() {
    int numberOfParticipants = 15;

    ExecutorService executorService = Executors.newFixedThreadPool(numberOfParticipants);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    List<Integer> numberOfParticipantsPerElectionLog = new ArrayList<>();

    for (int i = 0; i < numberOfParticipants; i++) {
      final int id = i;
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          new LeaderCandidate().run(id, numberOfParticipants, numberOfParticipantsPerElectionLog);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, executorService);

      futures.add(future);
    }

    futures.forEach(CompletableFuture::join);

    assertThat(numberOfParticipantsPerElectionLog).containsExactly(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1);
  }

}
