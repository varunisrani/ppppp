                summaries = await self.semantic_distiller.generate_summaries(script_text)

def show_storyboard():
    st.header("Storyboard")
    
    # Load results
    script_results = load_from_storage('script_ingestion_results.json')
    storyboard_results = load_from_storage('storyboard_results.json')
    
    if not script_results:
        st.warning("Please complete script analysis first.")
        return
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "Storyboard View", "Shot Setup", "Export Options", "Settings"
    ])
    
    with tab1:
        st.subheader("Storyboard View")
        
        if not storyboard_results:
            st.info("No storyboard generated yet.")
            if st.button("Generate Storyboard", type="primary"):
                with st.spinner("Generating storyboard..."):
                    try:
                        settings = load_from_storage('storyboard_settings.json') or {}
                        storyboard_results = asyncio.run(
                            storyboard_coordinator.generate_storyboard(
                                script_results,
                                shot_settings=settings.get("shot_settings", {})
                            )
                        )
                        save_to_storage(storyboard_results, 'storyboard_results.json')
                        st.success("Storyboard generated!")
                        st.rerun()
                    except Exception as e:
                        logger.error(f"Error generating storyboard: {str(e)}", exc_info=True)
                        st.error(f"An error occurred: {str(e)}")
        else:
            scenes = storyboard_results.get("scenes", [])
            if not scenes:
                st.warning("No scenes found in storyboard.")
                return
            
            # Initialize current scene index in session state if not present
            if 'current_scene_index' not in st.session_state:
                st.session_state.current_scene_index = 0
            
            current_scene = st.session_state.current_scene_index
            
            # Navigation controls
            col1, col2, col3 = st.columns([1, 4, 1])
            
            with col1:
                if st.button("⬅️ Previous") and current_scene > 0:
                    st.session_state.current_scene_index = current_scene - 1
                    st.rerun()
            
            with col2:
                scene = scenes[current_scene]
                st.write(f"### Scene {scene.get('scene_id', '?')}")
                
                # Display scene details
                if "image_path" in scene:
                    st.image(scene["image_path"], use_column_width=True)
                    
                    # Display scene description and technical notes
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Scene Description:**")
                        st.write(scene.get("description", "No description available"))
                    with col2:
                        st.write("**Technical Notes:**")
                        st.write(scene.get("technical_notes", "No technical notes available"))
                    
                    # Display existing annotations
                    st.write("**Annotations:**")
                    annotations = scene.get("annotations", [])
                    for annotation in annotations:
                        st.text(annotation)
                    
                    # Add new annotation
                    new_annotation = st.text_input(
                        "Add Annotation",
                        key=f"annotation_input_{scene['scene_id']}"
                    )
                    
                    if new_annotation:
                        if st.button("Add", key=f"slide_add_annot_{scene['scene_id']}"):
                            try:
                                storyboard_results = asyncio.run(
                                    storyboard_coordinator.add_annotation(
                                        storyboard_results,
                                        scene["scene_id"],
                                        new_annotation
                                    )
                                )
                                save_to_storage(storyboard_results, 'storyboard_results.json')
                                st.success("Annotation added!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error adding annotation: {str(e)}")
                else:
                    st.error(f"Scene {scene.get('scene_id', '?')} - No image path available")
            
            with col3:
                if st.button("Next ➡️") and current_scene < len(scenes) - 1:
                    st.session_state.current_scene_index = current_scene + 1
                    st.rerun()
            
            # Progress bar
            st.progress((current_scene + 1) / len(scenes))
            
            # Add regenerate button
            if st.button("Regenerate Storyboard", key="regenerate_button", type="primary"):
                with st.spinner("Regenerating storyboard images..."):
                    try:
                        settings = load_from_storage('storyboard_settings.json') or {}
                        storyboard_results = asyncio.run(
                            storyboard_coordinator.generate_storyboard(
                                script_results,
                                shot_settings=settings.get("shot_settings", {})
                            )
                        )
                        save_to_storage(storyboard_results, 'storyboard_results.json')
                        st.success("Storyboard regenerated!")
                        st.rerun()
                    except Exception as e:
                        logger.error(f"Error regenerating storyboard: {str(e)}", exc_info=True)
                        st.error(f"An error occurred: {str(e)}")
    
    with tab2:
        st.subheader("Shot Setup")
