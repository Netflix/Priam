package com.netflix.priam;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;

public class CompositeConfigSource extends AbstractConfigSource {

  private final ImmutableCollection<? extends ConfigSource> sources;

  public CompositeConfigSource(final ImmutableCollection<? extends ConfigSource> sources) {
    Preconditions.checkArgument(!sources.isEmpty(), "Can not create a composite config source without config sources!");
    this.sources = sources;
  }

  public CompositeConfigSource(final Collection<? extends ConfigSource> sources) {
    this(ImmutableList.copyOf(sources));
  }

  public CompositeConfigSource(final Iterable<? extends ConfigSource> sources) {
    this(ImmutableList.copyOf(sources));
  }

  @Override
  public void intialize(final String asgName, final String region) {
    for (final ConfigSource source : sources) {
      //TODO should this catch any potential exceptions?
      source.intialize(asgName, region);
    }
  }

  @Override
  public int size() {
    int size = 0;
    for (final ConfigSource c : sources) {
      size += c.size();
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean contains(final String key) {
    return get(key) != null;
  }

  @Override
  public String get(final String key) {
    Preconditions.checkNotNull(key);
    for (final ConfigSource c : sources) {
      final String value = c.get(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  @Override
  public void set(final String key, final String value) {
    Preconditions.checkNotNull(value, "Value can not be null for configurations.");
    //TODO should this write to the underline config sources, or a new memory based config source?
    final ConfigSource firstSource = Iterables.getFirst(sources, null);
    // firstSource shouldn't be null because the collection is immutable, and the collection is non empty.
    Preconditions.checkState(firstSource != null, "There was no ConfigSource found at the first location?");
    firstSource.set(key, value);
  }
}
