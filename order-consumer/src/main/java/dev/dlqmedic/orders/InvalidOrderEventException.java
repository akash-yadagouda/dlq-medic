package dev.dlqmedic.orders;

/** A payload that violates the `orders` contract. Never retried: it goes straight to the DLT. */
public class InvalidOrderEventException extends RuntimeException {

	public InvalidOrderEventException(String message) {
		super(message);
	}

}
