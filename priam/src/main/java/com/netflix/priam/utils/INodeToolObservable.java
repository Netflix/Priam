package com.netflix.priam.utils;

public interface INodeToolObservable {
	/*
	 * @param observer to add to list of internal observers.
	 */
	public void addObserver(INodeToolObserver observer);

	public void deleteObserver(INodeToolObserver observer);
}
