/*
 * Copyright 2023 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.asyncer.r2dbc.mysql;

/**
 * Provides a way to access an instance of a wrapped resource and for implementors to expose wrapped resources.
 *
 * @param <T> the wrapped resource
 */

public interface Wrapped<T> {
	
	/**
	 * Used to return an object that implements this interface or a wrapper for that object.
	 * 
	 * @return an instance of an object that implements this interface or a wrapper for that object.
	 */
	T unwrap();

}
